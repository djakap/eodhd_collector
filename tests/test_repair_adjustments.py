"""Deterministic DATA-003 repair planning and application tests."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

import flows.nightly_check_flow as nightly
from api.yfinance_client import RateLimitedError
from scripts import derive_4h
from scripts import repair_adjustments as repair
from scripts.check_adjustment_consistency import ScaleRun, StaleAction


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/data003"


def _stored(symbol):
    fixture = json.loads((FIXTURES / "stored_1h.json").read_text(encoding="utf-8"))
    return [tuple(row) for row in fixture["series"][symbol]]


def _fresh(symbol):
    return json.loads((FIXTURES / f"{symbol}_1h.json").read_text(encoding="utf-8"))


def _splits(symbol):
    fixture = json.loads((FIXTURES / "splits.json").read_text(encoding="utf-8"))
    return [tuple(row) for row in fixture["splits"][symbol]]


def test_t0_cuan_overlap_gate_pins_real_correction():
    decision = repair.decide_tail(_stored("CUAN.JK"), _fresh("CUAN.JK"), _splits("CUAN.JK"))

    assert decision.window_first == "2024-10-07T02:00:00"
    assert decision.overlap == 1368
    assert decision.edge_run == 1185
    assert decision.k == pytest.approx(0.1, abs=1e-9)
    assert decision.split == ("2025-07-15", "10/1")
    assert decision.operation == "multiply"
    assert decision.factor == 10.0
    assert decision.tail_bars == 224


def test_t1_kdsi_overlap_gate_pins_real_correction():
    decision = repair.decide_tail(_stored("KDSI.JK"), _fresh("KDSI.JK"), _splits("KDSI.JK"))

    assert decision.window_first == "2024-10-09T07:00:00"
    assert decision.overlap == 182
    assert decision.edge_run == 39
    assert decision.k == pytest.approx(0.25, abs=1e-9)
    assert decision.split == ("2024-11-07", "4/1")
    assert decision.operation == "multiply"
    assert decision.factor == 4.0
    assert decision.tail_bars == 34


@pytest.mark.parametrize("symbol", ["MLPT.JK", "FISH.JK"])
def test_t2_consistent_overlap_is_not_corrected(symbol):
    decision = repair.decide_tail(_stored(symbol), _fresh(symbol), _splits(symbol))

    assert decision.operation is None
    assert decision.reason == "k within 1% of 1"


def _gate_rows(count, ratio, *, first="2026-01-02T02:00:00"):
    start = datetime.fromisoformat(first)
    fresh = []
    stored = [("2026-01-01T02:00:00", 10.0, 10.0, 10.0, 10.0)]
    for offset in range(count):
        timestamp = start + timedelta(hours=offset)
        fresh.append(
            {
                "timestamp": timestamp.isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
            }
        )
        stored.append(
            (
                timestamp.isoformat(),
                100.0 * ratio,
                101.0 * ratio,
                99.0 * ratio,
                100.0 * ratio,
            )
        )
    return stored, fresh


def test_t3_synthetic_gate_failures_and_raw_direction():
    stored, fresh = _gate_rows(19, 0.1)
    assert repair.decide_tail(stored, fresh, [("2026-02-01", "10/1")]).reason == (
        "edge run too short"
    )

    stored, fresh = _gate_rows(20, 0.1)
    assert repair.decide_tail(stored, fresh, [("2025-12-01", "10/1")]).reason == (
        "no matching split in window"
    )
    assert repair.decide_tail(stored, fresh, []).reason == "no matching split in window"

    stored, fresh = _gate_rows(20, 0.3)
    assert repair.decide_tail(stored, fresh, [("2026-02-01", "4/1")]).reason == (
        "no matching split in window"
    )

    stored, fresh = _gate_rows(20, 4.0)
    raw = repair.decide_tail(stored, fresh, [("2026-02-01", "4/1")])
    assert raw.operation == "divide"
    assert raw.factor == 4.0


@pytest.mark.parametrize(
    ("operation", "expected"),
    [("multiply", (40.0, 44.0, 36.0, 42.0)), ("divide", (2.5, 2.75, 2.25, 2.625))],
)
def test_t4_correct_rows_preserves_non_price_fields(operation, expected):
    timestamp = datetime(2024, 8, 20, 2)
    original = (
        "SYN.JK",
        "1h",
        timestamp,
        10.0,
        11.0,
        9.0,
        10.5,
        None,
        1234,
        None,
        "intraday",
        datetime(2026, 7, 21),
    )
    now = datetime(2026, 10, 4, tzinfo=timezone.utc)
    decision = repair.TailDecision(
        "2024-10-01T02:00:00",
        20,
        20,
        0.25,
        ("2025-01-01", "4/1"),
        operation,
        4.0,
        1,
        "matched split",
    )

    row = repair.correct_rows([original], decision, now)[0]

    assert row[3:7] == expected
    assert row[:3] == original[:3]
    assert row[7:11] == original[7:11]
    assert row[11] == now
    assert len(row) == 12


def test_t5_select_targets_suppresses_registered_reasons_not_fish_symbol():
    fish_runs = [
        ScaleRun("FISH.JK", "1h", "2024-08-20", "2025-01-15", 63, 0.1),
        ScaleRun("FISH.JK", "4h", "2024-08-20", "2025-01-15", 63, 0.1),
    ]
    known = {
        (run.symbol, run.interval, run.first_day, run.last_day): "DATA-003"
        for run in fish_runs
    }
    assert repair.select_targets(fish_runs, [], known) == []

    dividend = StaleAction("FISH.JK", "dividend", "2027-06-12", {"d": 1})
    assert repair.select_targets(fish_runs, [dividend], known) == [
        repair.RepairTarget("FISH.JK", ("dividend 2027-06-12",), False)
    ]

    split = StaleAction("FISH.JK", "split", "2027-06-12", {"d": 1, "1h": 1})
    assert repair.select_targets(fish_runs, [split], known) == [
        repair.RepairTarget("FISH.JK", ("split 2027-06-12",), True)
    ]

    unknown = ScaleRun("AAA.JK", "1h", "2026-01-01", "2026-01-20", 20, 0.5)
    stale = StaleAction("ZZZ.JK", "dividend", "2026-09-01", {"d": 1})
    targets = repair.select_targets([unknown], [stale], known)
    assert targets == [
        repair.RepairTarget("AAA.JK", ("scale 1h 2026-01-01..2026-01-20",), True),
        repair.RepairTarget("ZZZ.JK", ("dividend 2026-09-01",), False),
    ]


def test_t6_series_hash_format_is_pinned():
    rows = [
        (datetime(2024, 8, 20, 2), 81.75, 82.0, 81.5, 81.75, None, 1000),
        (datetime(2024, 8, 20, 3), 81.75, 81.75, 81.75, 81.75, None, 0),
    ]
    assert repair.series_sha256(rows) == (
        "d963cff77e407417849babab9f8b566b1b74b5bd7dd62917551ed26a2c24ce98"
    )
    assert repair.series_sha256([]) == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


class _ReadOnlyCursor:
    def close(self):
        return None

    def execute(self, *_args, **_kwargs):
        raise AssertionError("plan fixture must not execute an unmodelled statement")


class _ReadOnlyConnection:
    def cursor(self):
        return _ReadOnlyCursor()

    def write(self, *_args, **_kwargs):
        raise AssertionError("plan must not write")


class _ReadOnlyProvider:
    def get_price_history(self, symbol, interval, **_kwargs):
        assert (symbol, interval) == ("SPLIT.JK", "1h")
        return _gate_rows(20, 0.25)[1]

    def write(self, *_args, **_kwargs):
        raise AssertionError("provider must not write")


def test_t7_plan_repairs_is_read_only_with_fakes(monkeypatch):
    run = ScaleRun("SPLIT.JK", "1h", "2026-01-01", "2026-01-20", 20, 0.25)
    monkeypatch.setattr(repair, "fetch_joined_days", lambda _cursor, interval: {"SPLIT.JK": []})
    monkeypatch.setattr(
        repair,
        "find_scale_runs",
        lambda _symbol, interval, _rows: [run] if interval == "1h" else [],
    )
    monkeypatch.setattr(repair, "fetch_stale_actions", lambda _cursor: [])
    monkeypatch.setattr(repair, "series_state", lambda _cursor, _symbol, _interval: (1, "hash"))
    stored, _fresh_rows = _gate_rows(20, 0.25)
    full_rows = [
        ("SPLIT.JK", "1h", datetime.fromisoformat(row[0]), *row[1:], None, 100, None,
         "intraday", datetime(2026, 7, 21))
        for row in stored
    ]
    monkeypatch.setattr(repair, "load_stored_1h", lambda _cursor, _symbol: full_rows)
    monkeypatch.setattr(repair, "load_splits", lambda _cursor, _symbol: [("2026-02-01", "4/1")])

    plan = repair.plan_repairs(
        _ReadOnlyConnection(),
        _ReadOnlyProvider(),
        today="2026-10-04",
    )

    assert plan["target_count"] == 1
    assert plan["targets"][0]["symbol"] == "SPLIT.JK"
    assert plan["targets"][0]["tail_decision"]["operation"] == "multiply"


class _ApplyDB:
    def __init__(self, calls):
        self.calls = calls
        self.cursor = object()

    def insert_price_data(self, rows, table):
        self.calls.append(("insert_tail", table, len(rows)))


class _ApplyCollector:
    def __init__(self, calls):
        self.calls = calls
        self.db = _ApplyDB(calls)

    def collect_eod(self, symbol):
        self.calls.append(("collect_eod", symbol))

    def _store(self, symbol, interval, rows):
        self.calls.append(("store", symbol, interval, len(rows)))


def test_t8_apply_plan_order_and_record(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(repair, "wait_for_wal", lambda _cursor: calls.append(("wait",)))
    monkeypatch.setattr(repair, "derive_4h", lambda *, symbol: calls.append(("derive", symbol)))
    monkeypatch.setattr(
        repair,
        "_state_dict",
        lambda _cursor, symbol, interval: calls.append(("state", symbol, interval))
        or {"rows": 2, "sha256": f"after-{symbol}-{interval}"},
    )
    before = lambda interval: {interval: {"before": {"rows": 1, "sha256": "before"}, "after": None}}
    decision = repair.TailDecision(
        "2024-10-01T02:00:00", 20, 20, 0.25, ("2025-01-01", "4/1"),
        "multiply", 4.0, 1, "matched split",
    )
    plan = repair.RepairPlan(
        mode="cli",
        today="2026-10-04",
        targets=[
            {
                "symbol": "DIV.JK",
                "reasons": ["dividend 2026-09-01"],
                "split_style": False,
                "intervals": {**before("d"), **before("w"), **before("m")},
                "tail_decision": None,
            },
            {
                "symbol": "SPLIT.JK",
                "reasons": ["split 2026-09-01"],
                "split_style": True,
                "intervals": {
                    **before("d"), **before("w"), **before("m"),
                    **before("1h"), **before("4h"),
                },
                "tail_decision": repair.asdict(decision),
            },
        ],
        errors=[],
    )
    plan.record_dir = tmp_path
    plan.fresh_1h["SPLIT.JK"] = [{"timestamp": "2024-10-01T02:00:00"}]
    plan.tail_rows["SPLIT.JK"] = [
        (
            "SPLIT.JK", "1h", datetime(2024, 8, 20, 2), 10.0, 11.0, 9.0, 10.5,
            None, 1000, None, "intraday", datetime(2026, 7, 21),
        )
    ]

    record = repair.apply_plan(
        plan,
        _ApplyCollector(calls),
        now=datetime(2026, 10, 4, tzinfo=timezone.utc),
    )

    assert calls[:5] == [
        ("collect_eod", "DIV.JK"),
        ("wait",),
        ("state", "DIV.JK", "d"),
        ("state", "DIV.JK", "w"),
        ("state", "DIV.JK", "m"),
    ]
    split_start = calls.index(("collect_eod", "SPLIT.JK"))
    assert calls[split_start:split_start + 6] == [
        ("collect_eod", "SPLIT.JK"),
        ("store", "SPLIT.JK", "1h", 1),
        ("insert_tail", "stock_data", 1),
        ("wait",),
        ("derive", "SPLIT.JK"),
        ("wait",),
    ]
    assert record["targets"][0]["tail_correction"] is None
    assert record["targets"][1]["tail_correction"]["originals"][0]["close"] == 10.5
    assert record["targets"][1]["intervals"]["4h"]["after"]["rows"] == 2
    assert [target["status"] for target in record["targets"]] == [
        "complete", "complete",
    ]
    assert Path(record["record_file"]).exists()


def test_t9_cli_refuses_recorded_or_fixed_date_apply_before_collector(monkeypatch, tmp_path):
    def forbidden_collector(*_args, **_kwargs):
        raise AssertionError("collector must not be constructed")

    monkeypatch.setattr(repair, "YFinancePriceCollector", forbidden_collector)
    assert repair.main(["--apply", "--recorded", str(tmp_path)]) == 2
    assert repair.main(["--apply", "--today", "2026-10-04"]) == 2


class _Log:
    def __init__(self):
        self.errors = []
        self.infos = []

    def error(self, message):
        self.errors.append(message)

    def info(self, message):
        self.infos.append(message)

    def warning(self, _message):
        return None


def _stub_flow(monkeypatch, order):
    healthy = {
        "settled": "2026-09-25", "total": 1, "coverage": 1,
        "coverage_ratio": 1.0, "laggards": [], "missing_entirely": [], "stale_days": 0,
    }
    monkeypatch.setattr(nightly, "get_run_logger", lambda: _Log())
    monkeypatch.setattr(nightly, "self_heal_wal", lambda: 0)
    monkeypatch.setattr(nightly, "check_partition_readability", lambda: 0)
    monkeypatch.setattr(nightly, "heal_adjustments", lambda: order.append("heal") or {"applied": 0})
    monkeypatch.setattr(
        nightly,
        "check_adjustment_consistency",
        lambda: order.append("check") or 0,
    )
    monkeypatch.setattr(nightly, "quality", lambda _table: {})
    monkeypatch.setattr(nightly, "check_completeness", lambda _path: healthy)
    monkeypatch.setattr(nightly, "heal", lambda _targets: {"attempted": 0, "healed": 0})


def test_t10_nightly_order_and_internal_heal_failure(monkeypatch):
    heal_adjustments_task = nightly.heal_adjustments
    order = []
    _stub_flow(monkeypatch, order)
    assert nightly.nightly_check_flow.fn("synthetic")["coverage"] == 1
    assert order == ["heal", "check"]

    monkeypatch.setattr(nightly, "heal_adjustments", heal_adjustments_task)

    log = _Log()
    monkeypatch.setattr(nightly, "get_run_logger", lambda: log)
    monkeypatch.setattr(
        nightly,
        "YFinancePriceCollector",
        type(
            "Collector",
            (),
            {
                "__init__": lambda self, update_mode: setattr(self, "api", object()),
                "__enter__": lambda self: self,
                "__exit__": lambda self, *_args: None,
            },
        ),
    )
    monkeypatch.setattr(nightly, "plan_repairs", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    summary = heal_adjustments_task.fn()
    assert summary["error"] == "RuntimeError: boom"
    assert log.errors == ["Adjustment heal gagal: RuntimeError: boom"]


def test_t11_nightly_cap_and_rate_limit_summary(monkeypatch, tmp_path):
    seen = {}
    calls = []
    log = _Log()
    monkeypatch.setattr(nightly, "get_run_logger", lambda: log)

    class Collector:
        def __init__(self):
            self.api = object()
            self.db = type("DB", (), {"cursor": object()})()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def collect_eod(self, symbol):
            calls.append(symbol)
            if symbol == "S01.JK":
                raise RateLimitedError("synthetic")

    collector = Collector()
    monkeypatch.setattr(
        nightly,
        "YFinancePriceCollector",
        lambda update_mode: collector if update_mode is False else None,
    )
    monkeypatch.setattr(repair, "wait_for_wal", lambda _cursor: None)
    monkeypatch.setattr(
        repair,
        "_state_dict",
        lambda _cursor, symbol, interval: {
            "rows": 1,
            "sha256": f"after-{symbol}-{interval}",
        },
    )

    def fake_plan(**kwargs):
        seen["cap"] = kwargs["cap"]
        targets = []
        for index in range(25):
            symbol = f"S{index:02d}.JK"
            targets.append(
                {
                    "symbol": symbol,
                    "reasons": ["dividend 2026-09-01"],
                    "split_style": False,
                    "intervals": {
                        interval: {
                            "before": {"rows": 1, "sha256": "before"},
                            "after": None,
                        }
                        for interval in ("d", "w", "m")
                    },
                    "tail_decision": None,
                }
            )
        plan = repair.RepairPlan(
            target_count=25,
            deferred=2,
            targets=targets,
            errors=[],
        )
        plan.record_dir = tmp_path
        return plan

    monkeypatch.setattr(nightly, "plan_repairs", fake_plan)

    summary = nightly.heal_adjustments.fn()

    assert seen["cap"] == repair.ADJUSTMENT_HEAL_CAP == 25
    assert summary["planned"] == 25
    assert summary["deferred"] == 2
    assert summary["applied"] == 1
    assert summary["error"] == "RateLimitedError: synthetic"
    assert calls == ["S00.JK", "S01.JK"]
    record = json.loads(Path(summary["record_file"]).read_text(encoding="utf-8"))
    assert [target["symbol"] for target in record["targets"]] == [
        "S00.JK", "S01.JK",
    ]
    assert [target["status"] for target in record["targets"]] == [
        "complete", "failed",
    ]
    assert record["targets"][1]["error"] == "RateLimitedError: synthetic"


def _write_ahead_plan(symbols, record_dir):
    decision = repair.TailDecision(
        "2024-10-01T02:00:00", 20, 20, 0.25,
        ("2025-01-01", "4/1"), "multiply", 4.0, 1, "matched split",
    )
    targets = []
    for symbol in symbols:
        targets.append(
            {
                "symbol": symbol,
                "reasons": ["split 2026-09-01"],
                "split_style": True,
                "intervals": {
                    interval: {
                        "before": {"rows": 1, "sha256": f"before-{symbol}-{interval}"},
                        "after": None,
                    }
                    for interval in ("d", "w", "m", "1h", "4h")
                },
                "tail_decision": repair.asdict(decision),
            }
        )

    plan = repair.RepairPlan(
        mode="cli",
        today="2026-10-04",
        targets=targets,
        errors=[],
    )
    plan.record_dir = record_dir
    for symbol in symbols:
        plan.fresh_1h[symbol] = [{"timestamp": "2024-10-01T02:00:00"}]
        plan.tail_rows[symbol] = [
            (
                symbol, "1h", datetime(2024, 8, 20, 2),
                10.0, 11.0, 9.0, 10.5, None, 1000, None,
                "intraday", datetime(2026, 7, 21),
            )
        ]
    return plan


@pytest.mark.parametrize("failed_index", [0, 1])
def test_t16_write_ahead_record_survives_tail_failure(
    monkeypatch, tmp_path, failed_index,
):
    symbols = ["FIRST.JK", "SECOND.JK"][:failed_index + 1]
    failed_symbol = symbols[-1]
    snapshots_at_insert = {}
    replacements = []
    real_replace = repair.os.replace

    def atomic_replace(source, destination):
        source = Path(source)
        destination = Path(destination)
        replacements.append((source, destination))
        assert source.parent == destination.parent == tmp_path
        assert source != destination
        real_replace(source, destination)

    monkeypatch.setattr(repair.os, "replace", atomic_replace)
    monkeypatch.setattr(repair, "wait_for_wal", lambda _cursor: None)
    monkeypatch.setattr(
        repair,
        "_state_dict",
        lambda _cursor, symbol, interval: {
            "rows": 2,
            "sha256": f"after-{symbol}-{interval}",
        },
    )

    def derive(*, symbol):
        if symbol == failed_symbol:
            raise RuntimeError(f"derive failed for {symbol}")

    monkeypatch.setattr(repair, "derive_4h", derive)

    class DB:
        cursor = object()

        def insert_price_data(self, rows, table):
            assert table == "stock_data"
            symbol = rows[0][0]
            files = list(tmp_path.glob("*_cli.json"))
            assert len(files) == 1
            snapshot = json.loads(files[0].read_text(encoding="utf-8"))
            target = next(
                item for item in snapshot["targets"]
                if item["symbol"] == symbol
            )
            assert target["status"] == "in_progress"
            assert target["tail_correction"]["originals"][0]["close"] == 10.5
            snapshots_at_insert[symbol] = snapshot

    class Collector:
        def __init__(self):
            self.db = DB()

        def collect_eod(self, _symbol):
            return None

        def _store(self, _symbol, _interval, _rows):
            return None

    plan = _write_ahead_plan(symbols, tmp_path)
    with pytest.raises(RuntimeError, match=f"derive failed for {failed_symbol}") as caught:
        repair.apply_plan(
            plan,
            Collector(),
            now=datetime(2026, 10, 4, tzinfo=timezone.utc),
        )

    record = caught.value.record
    record_path = Path(record["record_file"])
    disk_record = json.loads(record_path.read_text(encoding="utf-8"))
    expected_statuses = ["complete"] * failed_index + ["failed"]
    assert [target["status"] for target in disk_record["targets"]] == expected_statuses
    assert disk_record["targets"][-1]["error"] == (
        f"RuntimeError: derive failed for {failed_symbol}"
    )
    assert all(
        target["tail_correction"]["originals"][0]["close"] == 10.5
        for target in disk_record["targets"]
    )
    assert set(snapshots_at_insert) == set(symbols)
    assert replacements
    assert not list(tmp_path.glob("*.tmp"))


def test_t13_derive_default_sql_is_unchanged_and_symbol_is_parameterized(monkeypatch):
    class Cursor:
        def __init__(self):
            self.calls = []

        def execute(self, statement, parameters=None):
            self.calls.append((statement, parameters))

        def fetchone(self):
            return (7,)

    class Connection:
        def __init__(self):
            self.autocommit = False
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def close(self):
            return None

    connections = []

    def connect(**_kwargs):
        connection = Connection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(derive_4h.psycopg2, "connect", connect)
    monkeypatch.setattr(derive_4h.time, "sleep", lambda _seconds: None)
    where = (
        "interval='1h' AND hour(timestamp) IN (2,3,4,6,7,8,9) "
        "AND open IS NOT NULL"
    )
    expected_insert = f"""
        INSERT INTO {derive_4h.TABLE}
        SELECT symbol, '4h' as interval, {derive_4h.BUCKET} as ts,
            first(open), max(high), min(low), last(close), last(adjusted_close),
            sum(volume), 0, 'derived_4h', now()
        FROM {derive_4h.TABLE}
        WHERE {where}
        GROUP BY symbol, {derive_4h.BUCKET}
    """

    assert derive_4h.derive() == 7
    assert connections[0].cur.calls == [
        (expected_insert, None),
        ("SELECT count() FROM stock_data WHERE interval='4h'", None),
    ]

    assert derive_4h.derive(symbol="CUAN.JK") == 7
    insert_call, count_call = connections[1].cur.calls
    assert insert_call == (expected_insert.replace("AND open IS NOT NULL", "AND open IS NOT NULL AND symbol = %s"), ("CUAN.JK",))
    assert count_call == (
        "SELECT count() FROM stock_data WHERE interval='4h' AND symbol = %s",
        ("CUAN.JK",),
    )


def test_t14_recorded_provider_filters_and_missing_file_is_empty(tmp_path):
    provider = repair.RecordedProvider(FIXTURES)
    records = provider.get_price_history(
        "CUAN.JK",
        "1h",
        start="2024-10-08T00:00:00",
        end="2024-10-09T00:00:00",
    )
    assert records
    assert all(
        datetime(2024, 10, 8) <= datetime.fromisoformat(row["timestamp"]) < datetime(2024, 10, 9)
        for row in records
    )
    assert repair.RecordedProvider(tmp_path).get_price_history("MISSING.JK", "1h") == []


def test_t15_wait_for_wal_settles_and_times_out():
    class Cursor:
        def __init__(self, rows):
            self.rows = iter(rows)

        def execute(self, statement):
            assert statement.startswith("SELECT writerTxn")

        def fetchone(self):
            return next(self.rows)

    repair.wait_for_wal(
        Cursor([(1, 2), (2, 2)]),
        timeout=10,
        clock=iter([0, 0.5]).__next__,
        sleeper=lambda _seconds: None,
    )

    with pytest.raises(TimeoutError):
        repair.wait_for_wal(
            Cursor([(1, 2)]),
            timeout=1,
            clock=iter([0, 2]).__next__,
            sleeper=lambda _seconds: None,
        )
