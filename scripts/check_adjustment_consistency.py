#!/usr/bin/env python3
"""Report corporate-action adjustment defects in stock_data; never rewrite prices (QCF-004).

The scale and write-staleness predicates are documented in
docs/PRICE_ADJUSTMENT.md.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import os
import sys
from typing import Iterable, Mapping, Optional, Sequence

import psycopg2

sys.path[:0] = [os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]

from config.db_config import (
    QUESTDB_DATABASE,
    QUESTDB_HOST,
    QUESTDB_PASSWORD,
    QUESTDB_PG_PORT,
    QUESTDB_USER,
)
from config.tables import TABLE_ACTIONS_PRODUCTION, TABLE_PRICES_PRODUCTION


# QCF-004 §3.9 measures both stored intraday intervals against daily prices.
SCALE_INTERVALS = ("1h", "4h")
# QCF-004 §3.3 measures a 1% floor above the snapshot's p99 scale deviation.
SCALE_TOLERANCE = 0.01
# QCF-004 §3.9 separates stable scale faults from wandering price gaps at 1%.
SCALE_RUN_SPREAD = 0.01
# QCF-004 §3.9 places 15 days between the longest clean and shortest bad run.
SCALE_MIN_RUN_DAYS = 15
# QCF-004 §3.10 proves the NULL write generation reflects actions through this day.
NULL_CREATED_AT_AS_OF = datetime(2026, 7, 21, tzinfo=timezone.utc)
# QCF-004 §3.9 uses seven days to absorb recorded-action date uncertainty.
ACTION_DATE_MARGIN = timedelta(days=7)
# QCF-004 §3.9 measures intervals whose stored basis changes for each action type.
STALENESS_INTERVALS = {"split": ("d", "w", "m", "1h"), "dividend": ("d", "w", "m")}
# QCF-004 §3.9 pins the six scale faults observed in the restored snapshot.
KNOWN_SCALE_RUNS = {
    ("CUAN.JK", "1h", "2024-08-20", "2025-07-09"): "DATA-003",
    ("CUAN.JK", "4h", "2024-08-20", "2025-07-09"): "DATA-003",
    ("FISH.JK", "1h", "2024-08-20", "2025-01-15"): "DATA-003",
    ("FISH.JK", "4h", "2024-08-20", "2025-01-15"): "DATA-003",
    ("KDSI.JK", "1h", "2024-08-21", "2024-11-04"): "DATA-003",
    ("KDSI.JK", "4h", "2024-08-21", "2024-11-04"): "DATA-003",
}
# QCF-004 §3.9 observes all post-backfill dividend histories as write-stale.
KNOWN_STALENESS_CLASSES = {"dividend": "DATA-003"}


@dataclass(frozen=True)
class ScaleRun:
    symbol: str
    interval: str
    first_day: str
    last_day: str
    days: int
    ratio: float


@dataclass(frozen=True)
class StaleAction:
    symbol: str
    action_type: str
    action_date: str
    stale_rows: Mapping[str, int]


@dataclass(frozen=True)
class AdjustmentFinding:
    kind: str
    message: str
    problem: bool
    known_defect: bool


def _as_utc_datetime(value: date | datetime | str) -> datetime:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.combine(value, datetime.min.time())
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _wire_timestamp(value: date | datetime | str) -> str:
    return _as_utc_datetime(value).strftime("%Y-%m-%dT%H:%M:%S.000000Z")


def find_scale_runs(
    symbol: str,
    interval: str,
    joined_days: Sequence[tuple[str, Optional[float], Optional[float]]],
) -> list[ScaleRun]:
    """Return stable, persistent interval/daily scale mismatches."""
    found: list[ScaleRun] = []
    first_day: Optional[str] = None
    last_day: Optional[str] = None
    first_ratio: Optional[float] = None
    run_days = 0

    def close_run() -> None:
        nonlocal first_day, last_day, first_ratio, run_days
        if run_days >= SCALE_MIN_RUN_DAYS:
            found.append(
                ScaleRun(
                    symbol,
                    interval,
                    first_day,
                    last_day,
                    run_days,
                    round(first_ratio, 4),
                )
            )
        first_day = None
        last_day = None
        first_ratio = None
        run_days = 0

    for day, interval_close, daily_close in sorted(joined_days, key=lambda row: row[0]):
        if (
            interval_close is None
            or daily_close is None
            or interval_close <= 0
            or daily_close <= 0
        ):
            continue

        ratio = interval_close / daily_close
        off_scale = abs(ratio - 1.0) > SCALE_TOLERANCE
        continues = (
            off_scale
            and first_ratio is not None
            and abs(ratio / first_ratio - 1.0) <= SCALE_RUN_SPREAD
        )
        if continues:
            last_day = day
            run_days += 1
            continue

        close_run()
        if off_scale:
            first_day = day
            last_day = day
            first_ratio = ratio
            run_days = 1

    close_run()
    return found


def count_stale_rows(
    action_date: date | datetime | str,
    created_at_counts: Iterable[tuple[Optional[datetime], int]],
) -> int:
    """Count rows whose last write cannot include the supplied action."""
    action_at = _as_utc_datetime(action_date)
    total = 0
    for created_at, count in created_at_counts:
        if created_at is None:
            if action_at > NULL_CREATED_AT_AS_OF:
                total += int(count)
        elif _as_utc_datetime(created_at) < action_at:
            total += int(count)
    return total


def assess(
    scale_runs: Iterable[ScaleRun],
    stale_actions: Iterable[StaleAction],
    *,
    known_scale_runs=KNOWN_SCALE_RUNS,
    known_staleness=KNOWN_STALENESS_CLASSES,
) -> list[AdjustmentFinding]:
    """Classify measured defects against the explicit DATA-003 registry."""
    findings: list[AdjustmentFinding] = []
    observed_keys = set()

    for run in sorted(
        scale_runs,
        key=lambda item: (item.symbol, item.interval, item.first_day, item.last_day),
    ):
        key = (run.symbol, run.interval, run.first_day, run.last_day)
        observed_keys.add(key)
        tracking = known_scale_runs.get(key)
        if tracking:
            findings.append(
                AdjustmentFinding(
                    "scale_run",
                    f"scale {run.symbol} {run.interval} {run.first_day}..{run.last_day} "
                    f"days={run.days} ratio={run.ratio:.4f} ({tracking})",
                    problem=False,
                    known_defect=True,
                )
            )
        else:
            findings.append(
                AdjustmentFinding(
                    "scale_run",
                    f"scale {run.symbol} {run.interval} {run.first_day}..{run.last_day} "
                    f"days={run.days} ratio={run.ratio:.4f} (UNREGISTERED)",
                    problem=True,
                    known_defect=False,
                )
            )

    for key in sorted(set(known_scale_runs) - observed_keys):
        findings.append(
            AdjustmentFinding(
                "registered_scale_run_missing",
                f"registered scale run {key!r} is absent; "
                "up" "date KNOWN_SCALE_RUNS deliberately",
                problem=True,
                known_defect=False,
            )
        )

    for action in sorted(
        stale_actions,
        key=lambda item: (item.action_date, item.symbol, item.action_type),
    ):
        tracking = known_staleness.get(action.action_type)
        stale = dict(action.stale_rows)
        kind = f"{action.action_type}_stale"
        if tracking:
            findings.append(
                AdjustmentFinding(
                    kind,
                    f"{action.action_type} {action.symbol} {action.action_date} "
                    f"stale={stale} ({tracking})",
                    problem=False,
                    known_defect=True,
                )
            )
        else:
            findings.append(
                AdjustmentFinding(
                    kind,
                    f"{action.action_type} {action.symbol} {action.action_date} "
                    f"stale={stale} (UNREGISTERED)",
                    problem=True,
                    known_defect=False,
                )
            )

    counts = {
        kind: sum(1 for finding in findings if finding.kind == kind)
        for kind in (
            "scale_run",
            "registered_scale_run_missing",
            "dividend_stale",
            "split_stale",
        )
    }
    findings.append(
        AdjustmentFinding(
            "summary",
            "summary "
            + " ".join(f"{kind}={count}" for kind, count in counts.items())
            + f" problems={sum(finding.problem for finding in findings)}"
            + f" known_defects={sum(finding.known_defect for finding in findings)}",
            problem=False,
            known_defect=False,
        )
    )
    return findings


def fetch_joined_days(cursor, interval: str) -> dict[str, list[tuple[str, float, float]]]:
    """Fetch daily-aligned interval and daily closes using one read-only query."""
    assert interval in SCALE_INTERVALS
    cursor.execute(
        f"""WITH h AS (SELECT symbol, timestamp, last(close) AS hclose FROM {TABLE_PRICES_PRODUCTION}
           WHERE interval = '{interval}' SAMPLE BY 1d ALIGN TO CALENDAR),
     d AS (SELECT symbol, timestamp, close AS dclose FROM {TABLE_PRICES_PRODUCTION} WHERE interval = 'd')
SELECT h.symbol, h.timestamp, h.hclose, d.dclose
FROM h JOIN d ON h.symbol = d.symbol AND h.timestamp = d.timestamp"""
    )
    joined = defaultdict(list)
    for symbol, day, interval_close, daily_close in cursor.fetchall():
        joined[str(symbol)].append(
            (
                day.strftime("%Y-%m-%d"),
                float(interval_close) if interval_close is not None else None,
                float(daily_close) if daily_close is not None else None,
            )
        )
    return dict(joined)


def fetch_stale_actions(cursor) -> list[StaleAction]:
    """Fetch action histories and count rows written before each action."""
    cursor.execute(
        f"""SELECT symbol, action_type, action_date FROM {TABLE_ACTIONS_PRODUCTION}
WHERE action_date > %s AND action_type IN ('split', 'dividend') ORDER BY action_date, symbol""",
        (_wire_timestamp(NULL_CREATED_AT_AS_OF),),
    )
    actions = list(cursor.fetchall())
    stale_actions = []
    for symbol, action_type, action_date in actions:
        action_at = _as_utc_datetime(action_date)
        cutoff = _wire_timestamp(action_at - ACTION_DATE_MARGIN)
        stale_rows = {}
        for interval in STALENESS_INTERVALS[str(action_type)]:
            cursor.execute(
                f"""SELECT created_at, count() FROM {TABLE_PRICES_PRODUCTION}
WHERE symbol = %s AND interval = %s AND timestamp < %s""",
                (str(symbol), interval, cutoff),
            )
            count = count_stale_rows(action_at, cursor.fetchall())
            if count:
                stale_rows[interval] = count
        if stale_rows:
            stale_actions.append(
                StaleAction(
                    str(symbol),
                    str(action_type),
                    action_at.strftime("%Y-%m-%d"),
                    stale_rows,
                )
            )
    return stale_actions


def inspect_adjustment_consistency(connection=None) -> list[AdjustmentFinding]:
    """Inspect scale and write-staleness state using SELECT statements only."""
    owns_connection = connection is None
    if owns_connection:
        connection = psycopg2.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PG_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database=QUESTDB_DATABASE,
        )
    try:
        cursor = connection.cursor()
        scale_runs = []
        for interval in SCALE_INTERVALS:
            joined = fetch_joined_days(cursor, interval)
            for symbol in sorted(joined):
                scale_runs.extend(find_scale_runs(symbol, interval, joined[symbol]))
        stale_actions = fetch_stale_actions(cursor)
        cursor.close()
        return assess(scale_runs, stale_actions)
    finally:
        if owns_connection:
            connection.close()


def main() -> int:
    findings = inspect_adjustment_consistency()
    for finding in findings:
        prefix = "PROBLEM" if finding.problem else "KNOWN" if finding.known_defect else "INFO"
        print(f"{prefix} {finding.message}")
    return 1 if any(finding.problem for finding in findings) else 0


if __name__ == "__main__":
    raise SystemExit(main())
