#!/usr/bin/env python3
"""Plan and apply corporate-action price repairs with an audit record (DATA-003)."""

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Optional

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.yfinance_client import RateLimitedError, YFinanceClient
from collectors.yfinance_price_collector import YFinancePriceCollector
from config.db_config import (
    QUESTDB_DATABASE,
    QUESTDB_HOST,
    QUESTDB_PASSWORD,
    QUESTDB_PG_PORT,
    QUESTDB_USER,
)
from config.tables import TABLE_ACTIONS_PRODUCTION, TABLE_PRICES_PRODUCTION
from scripts.check_adjustment_consistency import (
    KNOWN_SCALE_RUNS,
    SCALE_INTERVALS,
    fetch_joined_days,
    fetch_stale_actions,
    find_scale_runs,
)
from scripts.derive_4h import derive as derive_4h


# DATA-003 §3.2 / D3: request window inside Yahoo's nominal 730-day limit.
HEAL_1H_LOOKBACK_DAYS = 728
# DATA-003 D4 permits at most 0.5% relative variation in the overlap edge run.
EDGE_TOLERANCE = 0.005
# DATA-003 D4 requires about three trading days before trusting an edge run.
EDGE_MIN_BARS = 20
# DATA-003 D4 treats a factor within 1% of one as already consistent.
NO_CHANGE_TOLERANCE = 0.01
# DATA-003 D4 allows 2% when matching an overlap factor to a recorded split.
SPLIT_MATCH_TOLERANCE = 0.02
# DATA-003 D8 bounds unattended nightly provider work.
ADJUSTMENT_HEAL_CAP = 25
# DATA-003 D5 bounds every wait for QuestDB WAL application.
WAL_WAIT_SECONDS = 120
# DATA-003 D9 keeps repair evidence on the backup volume by default.
ADJUSTMENT_REPAIR_DIR = os.getenv(
    "ADJUSTMENT_REPAIR_DIR", "/backup/adjustment_repairs"
)


@dataclass(frozen=True)
class RepairTarget:
    symbol: str
    reasons: tuple[str, ...]
    split_style: bool


@dataclass(frozen=True)
class TailDecision:
    window_first: Optional[str]
    overlap: int
    edge_run: int
    k: Optional[float]
    split: Optional[tuple[str, str]]
    operation: Optional[str]
    factor: Optional[float]
    tail_bars: int
    reason: str


class RepairPlan(dict):
    """JSON-able plan with apply-only data kept outside its mapping."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fresh_1h = {}
        self.tail_rows = {}
        self.record_dir = None


def _as_datetime(value) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _iso_seconds(value) -> str:
    return _as_datetime(value).strftime("%Y-%m-%dT%H:%M:%S")


def _wire_timestamp(value) -> str:
    return _as_datetime(value).strftime("%Y-%m-%dT%H:%M:%S.000000Z")


def select_targets(scale_runs, stale_actions, known_scale_runs) -> list[RepairTarget]:
    """Return repair targets; registered runs suppress reasons, never symbols."""
    reasons_by_symbol = {}
    split_symbols = set()

    for action in stale_actions:
        reason = f"{action.action_type} {action.action_date}"
        reasons_by_symbol.setdefault(action.symbol, set()).add(reason)
        if action.action_type == "split":
            split_symbols.add(action.symbol)

    for run in scale_runs:
        key = (run.symbol, run.interval, run.first_day, run.last_day)
        if key in known_scale_runs:
            continue
        reasons_by_symbol.setdefault(run.symbol, set()).add(
            f"scale {run.interval} {run.first_day}..{run.last_day}"
        )
        split_symbols.add(run.symbol)

    return [
        RepairTarget(
            symbol,
            tuple(sorted(reasons_by_symbol[symbol])),
            symbol in split_symbols,
        )
        for symbol in sorted(reasons_by_symbol)
    ]


def _record_value(record, name, position):
    if isinstance(record, dict):
        return record.get(name)
    return record[position]


def decide_tail(stored_1h, fresh_1h, splits) -> TailDecision:
    """Apply DATA-003's two-sided overlap gate to an older stored 1h tail."""
    fresh_by_timestamp = {
        _iso_seconds(record["timestamp"]): record
        for record in fresh_1h
    }
    if not fresh_by_timestamp:
        return TailDecision(None, 0, 0, None, None, None, None, 0, "provider data not recorded")

    window_first = min(fresh_by_timestamp)
    stored_by_timestamp = {
        _iso_seconds(row[0]): row
        for row in stored_1h
    }
    overlap_timestamps = sorted(set(stored_by_timestamp) & set(fresh_by_timestamp))
    tail_bars = sum(timestamp < window_first for timestamp in stored_by_timestamp)

    edge_ratios = []
    first_ratio = None
    for timestamp in overlap_timestamps:
        stored = stored_by_timestamp[timestamp]
        fresh = fresh_by_timestamp[timestamp]
        ratios = []
        for offset, name in enumerate(("open", "high", "low", "close"), start=1):
            stored_value = stored[offset]
            fresh_value = fresh.get(name)
            if stored_value is None or fresh_value in (None, 0):
                ratios = []
                break
            ratios.append(float(stored_value) / float(fresh_value))
        if not ratios:
            break
        if first_ratio is None:
            first_ratio = ratios[-1]
        if first_ratio == 0 or any(
            abs(ratio / first_ratio - 1.0) > EDGE_TOLERANCE
            for ratio in ratios
        ):
            break
        edge_ratios.append(ratios[-1])

    edge_run = len(edge_ratios)
    k = statistics.median(edge_ratios) if edge_ratios else None
    common = dict(
        window_first=window_first,
        overlap=len(overlap_timestamps),
        edge_run=edge_run,
        k=k,
        tail_bars=tail_bars,
    )
    if edge_run < EDGE_MIN_BARS:
        return TailDecision(**common, split=None, operation=None, factor=None,
                            reason="edge run too short")
    if abs(k - 1.0) <= NO_CHANGE_TOLERANCE:
        return TailDecision(**common, split=None, operation=None, factor=None,
                            reason="k within 1% of 1")

    window_day = window_first[:10]
    for action_date, split_ratio in sorted(splits):
        action_day = str(action_date)[:10]
        if action_day < window_day:
            continue
        try:
            split_from, split_to = str(split_ratio).split("/", 1)
            factor = float(split_from) / float(split_to)
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        if abs(k * factor - 1.0) <= SPLIT_MATCH_TOLERANCE:
            return TailDecision(
                **common,
                split=(action_day, str(split_ratio)),
                operation="multiply",
                factor=factor,
                reason="matched split",
            )
        if abs(k / factor - 1.0) <= SPLIT_MATCH_TOLERANCE:
            return TailDecision(
                **common,
                split=(action_day, str(split_ratio)),
                operation="divide",
                factor=factor,
                reason="matched split",
            )

    return TailDecision(**common, split=None, operation=None, factor=None,
                        reason="no matching split in window")


def correct_rows(stored_tail_rows, decision: TailDecision, now) -> list[tuple]:
    """Return corrected writer-order tuples while preserving non-price fields."""
    if decision.operation not in {"multiply", "divide"} or not decision.factor:
        return []
    corrected = []
    for row in stored_tail_rows:
        values = list(row)
        for index in range(3, 7):
            if values[index] is None:
                continue
            value = float(values[index])
            if decision.operation == "multiply":
                value *= decision.factor
            else:
                value /= decision.factor
            values[index] = round(value, 6)
        values[11] = now
        corrected.append(tuple(values))
    return corrected


def series_sha256(rows) -> str:
    """Hash price content using DATA-003 D7's stable serialisation."""
    digest = hashlib.sha256()
    for row in rows:
        timestamp, open_, high, low, close, adjusted_close, volume = row[:7]
        line = (
            f"{_as_datetime(timestamp):%Y-%m-%dT%H:%M:%S.%f}|{open_!r}|{high!r}|"
            f"{low!r}|{close!r}|{adjusted_close!r}|{volume!r}\n"
        )
        digest.update(line.encode("utf-8"))
    return digest.hexdigest()


def load_stored_1h(cursor, symbol):
    cursor.execute(
        f"""SELECT symbol, interval, timestamp, open, high, low, close, adjusted_close,
       volume, gmtoffset, source, created_at
FROM {TABLE_PRICES_PRODUCTION} WHERE symbol = %s AND interval = '1h'
ORDER BY timestamp""",
        (symbol,),
    )
    return list(cursor.fetchall())


def load_splits(cursor, symbol):
    cursor.execute(
        f"""SELECT action_date, split_ratio FROM {TABLE_ACTIONS_PRODUCTION}
WHERE symbol = %s AND action_type = 'split' ORDER BY action_date""",
        (symbol,),
    )
    return [
        (action_date.strftime("%Y-%m-%d"), str(split_ratio))
        for action_date, split_ratio in cursor.fetchall()
    ]


def series_state(cursor, symbol, interval):
    cursor.execute(
        f"""SELECT timestamp, open, high, low, close, adjusted_close, volume
FROM {TABLE_PRICES_PRODUCTION} WHERE symbol = %s AND interval = %s
ORDER BY timestamp""",
        (symbol, interval),
    )
    rows = list(cursor.fetchall())
    return len(rows), series_sha256(rows)


def wait_for_wal(
    cursor,
    timeout=WAL_WAIT_SECONDS,
    *,
    clock=time.monotonic,
    sleeper=time.sleep,
):
    deadline = clock() + timeout
    while True:
        cursor.execute(
            "SELECT writerTxn, sequencerTxn FROM wal_tables() WHERE name = 'stock_data'"
        )
        row = cursor.fetchone()
        if row and row[0] == row[1]:
            return
        if clock() >= deadline:
            raise TimeoutError("stock_data WAL did not settle before timeout")
        sleeper(0.5)


def _connect():
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PG_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DATABASE,
    )


def _decision_rows(stored_rows):
    return [
        (_iso_seconds(row[2]), row[3], row[4], row[5], row[6])
        for row in stored_rows
    ]


def _state_dict(cursor, symbol, interval):
    rows, sha256 = series_state(cursor, symbol, interval)
    return {"rows": rows, "sha256": sha256}


def plan_repairs(connection, provider, *, cap=None, today=None) -> RepairPlan:
    """Build a read-only repair plan from the QCF-004 guard's findings."""
    owns_connection = connection is None
    if owns_connection:
        connection = _connect()
    cursor = connection.cursor()
    planning_day = today or datetime.now(timezone.utc).date()
    if isinstance(planning_day, str):
        planning_day = date.fromisoformat(planning_day)

    try:
        scale_runs = []
        for interval in SCALE_INTERVALS:
            joined = fetch_joined_days(cursor, interval)
            for symbol in sorted(joined):
                scale_runs.extend(find_scale_runs(symbol, interval, joined[symbol]))
        stale_actions = fetch_stale_actions(cursor)
        all_targets = select_targets(scale_runs, stale_actions, KNOWN_SCALE_RUNS)
        selected = all_targets if cap is None else all_targets[:cap]

        plan = RepairPlan(
            mode="dry_run",
            today=planning_day.isoformat(),
            total_targets=len(all_targets),
            target_count=len(selected),
            deferred=max(0, len(all_targets) - len(selected)),
            targets=[],
            errors=[],
        )
        for target in selected:
            try:
                intervals = ["d", "w", "m"]
                if target.split_style:
                    intervals.extend(["1h", "4h"])
                item = {
                    "symbol": target.symbol,
                    "reasons": list(target.reasons),
                    "split_style": target.split_style,
                    "intervals": {
                        interval: {
                            "before": _state_dict(cursor, target.symbol, interval),
                            "after": None,
                        }
                        for interval in intervals
                    },
                    "tail_decision": None,
                }
                if target.split_style:
                    stored_rows = load_stored_1h(cursor, target.symbol)
                    start = planning_day - timedelta(days=HEAL_1H_LOOKBACK_DAYS)
                    fresh = provider.get_price_history(
                        target.symbol,
                        "1h",
                        start=start.isoformat(),
                    )
                    decision = decide_tail(
                        _decision_rows(stored_rows),
                        fresh,
                        load_splits(cursor, target.symbol),
                    )
                    item["tail_decision"] = asdict(decision)
                    plan.fresh_1h[target.symbol] = fresh
                    if decision.window_first:
                        first = _as_datetime(decision.window_first)
                        plan.tail_rows[target.symbol] = [
                            row for row in stored_rows if _as_datetime(row[2]) < first
                        ]
                    else:
                        plan.tail_rows[target.symbol] = []
                plan["targets"].append(item)
            except RateLimitedError:
                raise
            except Exception as exc:
                plan["errors"].append(
                    f"{target.symbol}: {type(exc).__name__}: {exc}"
                )
        return plan
    finally:
        if hasattr(cursor, "close"):
            cursor.close()
        if owns_connection:
            connection.close()


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _write_json_record(record, directory, now, suffix):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = _as_datetime(now).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"{stamp}_{suffix}.json"
    temporary = directory / (
        f".{path.name}.{os.getpid()}.{time.monotonic_ns()}.tmp"
    )
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(record, indent=2, default=_json_default) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return str(path)


def _original_ohlc(rows):
    return [
        {
            "timestamp": _iso_seconds(row[2]),
            "open": row[3],
            "high": row[4],
            "low": row[5],
            "close": row[6],
        }
        for row in rows
    ]


def apply_plan(plan: RepairPlan, collector, *, now=None) -> dict:
    """Apply a live-provider plan and persist before/after repair evidence."""
    applied_at = now or datetime.now(timezone.utc)
    mode = plan.get("mode") if plan.get("mode") in {"cli", "nightly"} else "cli"
    record = {
        "mode": mode,
        "applied_at": applied_at.isoformat(),
        "today": plan.get("today"),
        "targets": [],
        "errors": list(plan.get("errors", [])),
    }
    record_dir = plan.record_dir or ADJUSTMENT_REPAIR_DIR
    current_target = None

    try:
        for item in plan.get("targets", []):
            symbol = item["symbol"]
            current_target = {
                "symbol": symbol,
                "reasons": list(item["reasons"]),
                "split_style": item["split_style"],
                "intervals": item["intervals"],
                "tail_correction": None,
                "status": "in_progress",
            }
            record["targets"].append(current_target)
            record["record_file"] = _write_json_record(
                record, record_dir, applied_at, mode
            )

            collector.collect_eod(symbol)
            if item["split_style"]:
                fresh = plan.fresh_1h.get(symbol, [])
                collector._store(symbol, "1h", fresh)
                decision = TailDecision(**item["tail_decision"])
                tail_rows = plan.tail_rows.get(symbol, [])
                corrected = correct_rows(tail_rows, decision, applied_at)
                if corrected:
                    current_target["tail_correction"] = {
                        **item["tail_decision"],
                        "originals": _original_ohlc(tail_rows),
                    }
                    record["record_file"] = _write_json_record(
                        record, record_dir, applied_at, mode
                    )
                    collector.db.insert_price_data(
                        corrected,
                        table=TABLE_PRICES_PRODUCTION,
                    )

            wait_for_wal(collector.db.cursor)
            if item["split_style"]:
                derive_4h(symbol=symbol)
                wait_for_wal(collector.db.cursor)
            for interval in current_target["intervals"]:
                current_target["intervals"][interval]["after"] = _state_dict(
                    collector.db.cursor,
                    symbol,
                    interval,
                )
            current_target["status"] = "complete"
            record["record_file"] = _write_json_record(
                record, record_dir, applied_at, mode
            )
            current_target = None
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        record["errors"].append(error)
        if current_target is not None:
            current_target["status"] = "failed"
            current_target["error"] = error
            record["record_file"] = _write_json_record(
                record, record_dir, applied_at, mode
            )
        exc.record = record
        raise

    return record


class RecordedProvider:
    """Read planner-recorded price responses without constructing a live client."""

    def __init__(self, directory):
        self.directory = Path(directory)

    def get_price_history(self, symbol, interval, start=None, end=None, period=None):
        path = self.directory / f"{symbol}_{interval}.json"
        if not path.exists():
            return []
        records = json.loads(path.read_text(encoding="utf-8"))
        start_at = _as_datetime(start) if start else None
        end_at = _as_datetime(end) if end else None
        return [
            record
            for record in records
            if (start_at is None or _as_datetime(record["timestamp"]) >= start_at)
            and (end_at is None or _as_datetime(record["timestamp"]) < end_at)
        ]


def _print_json(value):
    print(json.dumps(value, indent=2, default=_json_default))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorded", help="directory containing recorded provider JSON")
    parser.add_argument("--today", help="fixed planning date (YYYY-MM-DD)")
    parser.add_argument("--record-dir", help="override evidence-record directory")
    parser.add_argument("--apply", action="store_true", help="apply a live-provider plan")
    parser.add_argument("--cap", type=int, help="maximum number of targets")
    args = parser.parse_args(argv)

    if args.apply and (args.recorded or args.today):
        print("REFUSED: --apply cannot be combined with --recorded or --today", file=sys.stderr)
        return 2

    try:
        if args.apply:
            with YFinancePriceCollector(update_mode=False) as collector:
                plan = plan_repairs(None, collector.api, cap=args.cap)
                plan["mode"] = "cli"
                plan.record_dir = args.record_dir
                record = apply_plan(plan, collector)
                _print_json(record)
                return 1 if record["errors"] else 0

        provider = RecordedProvider(args.recorded) if args.recorded else YFinanceClient()
        plan = plan_repairs(None, provider, cap=args.cap, today=args.today)
        if args.record_dir:
            plan_file = _write_json_record(
                plan,
                args.record_dir,
                datetime.now(timezone.utc),
                "dry_run",
            )
            plan["record_file"] = plan_file
        _print_json(plan)
        return 1 if plan["errors"] else 0
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
