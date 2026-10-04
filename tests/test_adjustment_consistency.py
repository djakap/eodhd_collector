"""Deterministic regression coverage for QCF-004 adjustment consistency."""

import ast
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re

import pytest

import flows.nightly_check_flow as nightly
from scripts import check_adjustment_consistency as adjustment
from scripts.check_adjustment_consistency import (
    AdjustmentFinding,
    KNOWN_SCALE_RUNS,
    KNOWN_STALENESS_CLASSES,
    ScaleRun,
    StaleAction,
    assess,
    count_stale_rows,
    fetch_stale_actions,
    find_scale_runs,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/adjustment"


def _scale_fixture():
    return json.loads((FIXTURES / "scale_1h.json").read_text(encoding="utf-8"))["series"]


def _rows(series, symbol):
    return [tuple(row) for row in series[symbol]]


def _synthetic(days, ratio, *, start=datetime(2026, 1, 1)):
    return [
        ((start + timedelta(days=offset)).strftime("%Y-%m-%d"), ratio * 100.0, 100.0)
        for offset in range(days)
    ]


def test_t0_cuan_fixture_pins_the_full_broken_run():
    series = _scale_fixture()

    assert find_scale_runs("CUAN.JK", "1h", _rows(series, "CUAN.JK")) == [
        ScaleRun("CUAN.JK", "1h", "2024-08-20", "2025-07-09", 207, 0.1)
    ]


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        (
            "KDSI.JK",
            ScaleRun("KDSI.JK", "1h", "2024-08-21", "2024-11-04", 30, 0.25),
        ),
        (
            "FISH.JK",
            ScaleRun("FISH.JK", "1h", "2024-08-20", "2025-01-15", 63, 0.1),
        ),
    ],
)
def test_t1_other_broken_fixtures_pin_their_runs(symbol, expected):
    series = _scale_fixture()

    assert find_scale_runs(symbol, "1h", _rows(series, symbol)) == [expected]


@pytest.mark.parametrize("symbol", ["MLPT.JK", "DSSA.JK", "RMKE.JK", "KLAS.JK", "CYBR.JK"])
def test_t2_representative_split_fixtures_are_clean(symbol):
    series = _scale_fixture()

    assert find_scale_runs(symbol, "1h", _rows(series, symbol)) == []


def test_t3_minimum_run_boundary_and_mina_short_run():
    series = _scale_fixture()

    assert find_scale_runs("MINA.JK", "1h", _rows(series, "MINA.JK")) == []
    assert find_scale_runs("SYN.JK", "1h", _synthetic(14, 1.2378)) == []
    assert find_scale_runs("SYN.JK", "1h", _synthetic(15, 1.2378)) == [
        ScaleRun("SYN.JK", "1h", "2026-01-01", "2026-01-15", 15, 1.2378)
    ]


def test_t4_scale_floor_is_explicit():
    visible = 1 / 1.0217
    below_floor = 1 / 1.0053

    assert find_scale_runs("VISIBLE.JK", "1h", _synthetic(40, visible)) == [
        ScaleRun("VISIBLE.JK", "1h", "2026-01-01", "2026-02-09", 40, round(visible, 4))
    ]
    assert find_scale_runs("FLOOR.JK", "1h", _synthetic(40, below_floor)) == []


def test_t5_on_scale_ratio_changes_and_skipped_closes_define_runs():
    on_scale_split = _synthetic(15, 0.1)
    on_scale_split.append(("2026-01-16", 100.0, 100.0))
    on_scale_split.extend(_synthetic(15, 0.1, start=datetime(2026, 1, 17)))
    runs = find_scale_runs("SPLIT.JK", "1h", on_scale_split)
    assert [(run.first_day, run.last_day, run.days) for run in runs] == [
        ("2026-01-01", "2026-01-15", 15),
        ("2026-01-17", "2026-01-31", 15),
    ]

    ratio_change = _synthetic(15, 0.1)
    ratio_change.extend(_synthetic(15, 0.102, start=datetime(2026, 1, 16)))
    assert [run.ratio for run in find_scale_runs("RATIO.JK", "1h", ratio_change)] == [
        0.1,
        0.102,
    ]

    skipped = _synthetic(7, 0.1)
    skipped.append(("2026-01-08", None, 100.0))
    skipped.extend(_synthetic(8, 0.1, start=datetime(2026, 1, 9)))
    assert find_scale_runs("SKIP.JK", "1h", skipped) == [
        ScaleRun("SKIP.JK", "1h", "2026-01-01", "2026-01-16", 15, 0.1)
    ]


def test_t6_stale_count_handles_null_boundaries_and_timezones():
    assert count_stale_rows(datetime(2026, 7, 21), [(None, 5)]) == 0
    assert count_stale_rows(datetime(2026, 7, 22), [(None, 5)]) == 5

    action_aware = datetime(2026, 8, 11, tzinfo=timezone.utc)
    action_naive = datetime(2026, 8, 11)
    groups = [
        (None, 10),
        (datetime(2026, 8, 10), 2),
        (datetime(2026, 8, 11), 3),
        (datetime(2026, 8, 12, tzinfo=timezone.utc), 4),
    ]
    assert count_stale_rows(action_aware, groups) == 12
    assert count_stale_rows(action_naive, groups) == 12


def test_t7_staleness_fixture_pins_ikbi_and_cmry_counts():
    fixture = json.loads((FIXTURES / "staleness.json").read_text(encoding="utf-8"))
    expected = {
        "IKBI.JK": {"d": 6008, "w": 1265, "m": 291},
        "CMRY.JK": {"d": 1132, "w": 245, "m": 57},
    }

    for action in fixture["actions"]:
        counts = {}
        for interval, raw_groups in action["groups"].items():
            groups = [
                (
                    None
                    if created_at is None
                    else datetime.fromisoformat(created_at.replace("Z", "+00:00")),
                    count,
                )
                for created_at, count in raw_groups
            ]
            counts[interval] = count_stale_rows(action["action_date"], groups)
        assert counts == expected[action["symbol"]]


def _registered_runs():
    details = {
        "CUAN.JK": (207, 0.1),
        "FISH.JK": (63, 0.1),
        "KDSI.JK": (30, 0.25),
    }
    return [
        ScaleRun(symbol, interval, first_day, last_day, details[symbol][0], details[symbol][1])
        for symbol, interval, first_day, last_day in KNOWN_SCALE_RUNS
    ]


def test_t8_assess_classifies_registered_unregistered_and_vanished_runs():
    registered = assess(_registered_runs(), [])
    scale_findings = [finding for finding in registered if finding.kind == "scale_run"]
    assert len(scale_findings) == 2
    assert all(finding.known_defect and not finding.problem for finding in scale_findings)
    assert all("DATA-003" in finding.message for finding in scale_findings)

    unknown = ScaleRun("NEW.JK", "1h", "2026-01-01", "2026-01-20", 20, 0.5)
    unregistered = assess([unknown], [], known_scale_runs={})[0]
    assert unregistered.problem and not unregistered.known_defect
    assert "UNREGISTERED" in unregistered.message

    key = ("GONE.JK", "1h", "2026-01-01", "2026-01-20")
    vanished = assess([], [], known_scale_runs={key: "DATA-003"})[0]
    assert vanished.kind == "registered_scale_run_missing"
    assert vanished.problem
    assert "update KNOWN_SCALE_RUNS" in vanished.message


def test_t9_assess_classifies_stale_action_types():
    dividend = StaleAction("DIV.JK", "dividend", "2026-08-11", {"d": 10})
    split = StaleAction("SPLIT.JK", "split", "2026-08-11", {"d": 10, "1h": 4})

    known = assess(
        [],
        [dividend],
        known_scale_runs={},
        known_staleness={"dividend": "DATA-003"},
    )[0]
    assert known.kind == "dividend_stale"
    assert known.known_defect and not known.problem
    assert "DATA-003" in known.message

    unknown_split = assess([], [split], known_scale_runs={})[0]
    assert unknown_split.kind == "split_stale"
    assert unknown_split.problem
    assert "UNREGISTERED" in unknown_split.message

    unknown_dividend = assess(
        [], [dividend], known_scale_runs={}, known_staleness={}
    )[0]
    assert unknown_dividend.problem


class _Log:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.infos = []

    def error(self, message):
        self.errors.append(message)

    def warning(self, message):
        self.warnings.append(message)

    def info(self, message):
        self.infos.append(message)


def _stub_healthy_flow(monkeypatch, adjustment_problems):
    healthy = {
        "settled": "2026-09-25",
        "total": 1,
        "coverage": 1,
        "coverage_ratio": 1.0,
        "laggards": [],
        "missing_entirely": [],
        "stale_days": 0,
    }
    monkeypatch.setattr(nightly, "self_heal_wal", lambda: 0)
    monkeypatch.setattr(nightly, "check_partition_readability", lambda: 0)
    monkeypatch.setattr(nightly, "heal_adjustments", lambda: {})
    monkeypatch.setattr(nightly, "check_adjustment_consistency", lambda: adjustment_problems)
    monkeypatch.setattr(nightly, "quality", lambda _table: {})
    monkeypatch.setattr(nightly, "check_completeness", lambda _path: healthy)
    monkeypatch.setattr(nightly, "heal", lambda _candidates: {"attempted": 0, "healed": 0})


def test_t10_nightly_task_logs_findings_and_flow_uses_problem_count(monkeypatch):
    log = _Log()
    findings = [
        AdjustmentFinding("scale_run", "problem one", True, False),
        AdjustmentFinding("split_stale", "problem two", True, False),
        AdjustmentFinding("dividend_stale", "known one", False, True),
    ]
    monkeypatch.setattr(nightly, "get_run_logger", lambda: log)
    monkeypatch.setattr(nightly, "inspect_adjustment_consistency", lambda: findings)

    assert nightly.check_adjustment_consistency.fn() == 2
    assert log.errors == ["problem one", "problem two"]
    assert log.warnings == ["known one"]

    _stub_healthy_flow(monkeypatch, 2)
    with pytest.raises(RuntimeError, match="2 temuan penyesuaian harga belum terdaftar"):
        nightly.nightly_check_flow.fn("synthetic")

    monkeypatch.setattr(nightly, "check_adjustment_consistency", lambda: 0)
    assert nightly.nightly_check_flow.fn("synthetic")["coverage"] == 1


def test_t11_nightly_task_converts_inspection_exception_to_one_problem(monkeypatch):
    log = _Log()
    monkeypatch.setattr(nightly, "get_run_logger", lambda: log)

    def fail():
        raise RuntimeError("synthetic connection failure")

    monkeypatch.setattr(nightly, "inspect_adjustment_consistency", fail)
    assert nightly.check_adjustment_consistency.fn() == 1
    assert len(log.errors) == 1
    assert "RuntimeError: synthetic connection failure" in log.errors[0]


WRITE_STATEMENT = re.compile(
    r"\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|DROP\s+\w+|"
    r"ALTER\s+\w+|TRUNCATE\s+\w+|CREATE\s+\w+)\b",
    re.IGNORECASE,
)


def _execute_statements(source):
    statements = []
    for node in ast.walk(ast.parse(source)):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute"
            and node.args
        ):
            continue
        statement = node.args[0]
        if isinstance(statement, ast.Constant) and isinstance(statement.value, str):
            statements.append(statement.value)
        elif isinstance(statement, ast.JoinedStr):
            statements.append(
                "".join(
                    part.value
                    for part in statement.values
                    if isinstance(part, ast.Constant) and isinstance(part.value, str)
                )
            )
    return statements


def _write_statements(source):
    return [statement for statement in _execute_statements(source) if WRITE_STATEMENT.search(statement)]


def test_t12_inspection_executes_only_select_statements_with_negative_control():
    source = (ROOT / "scripts/check_adjustment_consistency.py").read_text(encoding="utf-8")
    statements = _execute_statements(source)

    assert len(statements) >= 3
    assert all(statement.strip().upper().startswith(("SELECT", "WITH")) for statement in statements)
    assert _write_statements(source) == []

    unsafe = 'cursor.execute("UPDATE stock_data SET close = 1")'
    assert _write_statements(unsafe) == ["UPDATE stock_data SET close = 1"]


class _FakeCursor:
    def __init__(self):
        self.calls = []
        self.result = None

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))
        if "action_type" in statement:
            self.result = [("WIRE.JK", "dividend", datetime(2026, 8, 11))]
        else:
            self.result = [
                (None, 4),
                (datetime(2026, 8, 10, 12, 0), 2),
                (datetime(2026, 8, 11, 12, 0), 1),
            ]

    def fetchall(self):
        return self.result


def test_t13_wire_parameters_are_strings_and_naive_results_are_supported():
    cursor = _FakeCursor()

    actions = fetch_stale_actions(cursor)

    assert actions == [
        StaleAction("WIRE.JK", "dividend", "2026-08-11", {"d": 6, "w": 6, "m": 6})
    ]
    wire = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000000Z$")
    allowed = {"WIRE.JK", "d", "w", "m"}
    for _statement, parameters in cursor.calls:
        for parameter in parameters or ():
            assert isinstance(parameter, str)
            assert parameter in allowed or wire.fullmatch(parameter)


def test_t14_registries_are_exactly_the_approved_data_003_entries():
    assert set(KNOWN_SCALE_RUNS) == {
        ("FISH.JK", "1h", "2024-08-20", "2025-01-15"),
        ("FISH.JK", "4h", "2024-08-20", "2025-01-15"),
    }
    assert set(KNOWN_SCALE_RUNS.values()) == {"DATA-003"}
    assert KNOWN_STALENESS_CLASSES == {}
