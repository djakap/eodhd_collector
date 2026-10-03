"""Regression coverage for QCF-016 partition detection and crash evidence."""

from collections import Counter
import logging

import pytest

import flows.nightly_check_flow as nightly
from scripts.check_table_partitions import (
    assess_partition_state,
    inspect_all_tables,
)
from scripts.classify_questdb_crashes import classify_directory


def test_t0_zero_disk_partition_is_a_problem_and_names_the_partition():
    result = assess_partition_state(
        "prices",
        [("2025", 10), ("2026", 20)],
        readable_rows=30,
        suspended=False,
        disk_sizes=[("2025", 0), ("2026", 4096)],
    )

    assert result.problem is True
    assert "2025" in result.message
    assert "2026" not in result.message
    assert "diskSize=0" in result.message


def test_t1_omitted_disk_sizes_preserve_qcf013_healthy_message():
    result = assess_partition_state(
        "corporate_actions",
        [("2000", 7), ("2001", 20)],
        readable_rows=27,
        suspended=False,
    )

    assert result.message == "corporate_actions: healthy (27 rows across 2 partitions)"


def test_t2_known_count_divergence_warns_without_alerting(monkeypatch, caplog):
    result = assess_partition_state(
        "yf_fundamentals",
        [("2021", 100), ("2022", 100)],
        readable_rows=150,
        suspended=False,
        disk_sizes=[("2021", 1024), ("2022", 1024)],
    )

    assert result.problem is False
    assert result.known_defect is True
    assert "DATA-002" in result.message

    monkeypatch.setattr(nightly, "inspect_all_tables", lambda: [result])
    monkeypatch.setattr(
        nightly,
        "get_run_logger",
        lambda: logging.getLogger("qcf016-known-divergence"),
    )
    with caplog.at_level(logging.WARNING):
        assert nightly.check_partition_readability.fn() == 0
    assert "DATA-002" in caplog.text


def test_t3_known_divergence_is_not_a_blanket_pass():
    missing_partition = assess_partition_state(
        "yf_fundamentals",
        [("2024", 100)],
        readable_rows=100,
        suspended=False,
        disk_sizes=[("2024", 0)],
    )
    unreadable = assess_partition_state(
        "yf_fundamentals",
        [("2024", 100)],
        readable_rows=None,
        suspended=False,
        readability_error="DatabaseError: missing partition",
        disk_sizes=[("2024", 1024)],
    )

    assert missing_partition.problem is True
    assert unreadable.problem is True


def test_t4_unlisted_count_divergence_remains_a_problem():
    result = assess_partition_state(
        "another_table",
        [("2026", 100)],
        readable_rows=99,
        suspended=False,
        disk_sizes=[("2026", 1024)],
    )

    assert result.problem is True
    assert "PARTITION DIVERGENCE" in result.message


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.statement = ""
        self.params = None

    def execute(self, statement, params=None):
        self.statement = " ".join(statement.split())
        self.params = params
        self.connection.statements.append(self.statement)

    def fetchall(self):
        if "FROM tables()" in self.statement:
            return [("alpha",), ("beta",)]
        if "table_partitions" in self.statement:
            table = self.params[0]
            return [("2026", self.connection.rows[table], 4096)]
        raise AssertionError(f"unexpected fetchall: {self.statement}")

    def fetchone(self):
        if "wal_tables()" in self.statement:
            return (False,)
        if self.statement.startswith("SELECT count()"):
            table = self.statement.rsplit('"', 2)[1]
            return (self.connection.rows[table],)
        raise AssertionError(f"unexpected fetchone: {self.statement}")


class _FakeConnection:
    rows = {"alpha": 10, "beta": 20}

    def __init__(self):
        self.statements = []
        self.rollback_called = False

    def cursor(self):
        return _FakeCursor(self)

    def rollback(self):
        self.rollback_called = True


def test_t5_inspect_all_tables_discovers_each_table_from_tables_function():
    connection = _FakeConnection()

    findings = inspect_all_tables(connection=connection)

    assert [finding.table for finding in findings] == ["alpha", "beta"]
    assert [finding.readable_rows for finding in findings] == [10, 20]
    assert all(not finding.problem for finding in findings)
    assert connection.statements[0] == "SELECT table_name FROM tables() ORDER BY table_name"
    assert all(statement.startswith("SELECT") for statement in connection.statements)


def _stub_nightly(monkeypatch, partition_problem_count):
    healthy = {
        "settled": "2026-09-25",
        "total": 1,
        "coverage": 1,
        "coverage_ratio": 1.0,
        "laggards": [],
        "missing_entirely": [],
        "stale_days": 0,
    }
    monkeypatch.setattr(nightly, "get_run_logger", lambda: logging.getLogger("qcf016-alert"))
    monkeypatch.setattr(nightly, "self_heal_wal", lambda: 0)
    monkeypatch.setattr(
        nightly,
        "check_partition_readability",
        lambda: partition_problem_count,
    )
    monkeypatch.setattr(nightly, "check_adjustment_consistency", lambda: 0)
    monkeypatch.setattr(nightly, "quality", lambda _table: {})
    monkeypatch.setattr(nightly, "check_completeness", lambda _path: healthy)
    monkeypatch.setattr(nightly, "heal", lambda _candidates: {"attempted": 0, "healed": 0})


def test_t6_partition_problem_count_controls_nightly_alert(monkeypatch):
    _stub_nightly(monkeypatch, 2)
    with pytest.raises(RuntimeError, match="2 tabel bermasalah"):
        nightly.nightly_check_flow.fn("synthetic")

    _stub_nightly(monkeypatch, 0)
    result = nightly.nightly_check_flow.fn("synthetic")
    assert result["coverage"] == 1


def _jvm_log(signal, code, frame, *, timestamp="Tue Jun  2 01:28:52 2026 UTC"):
    fatal = f"#  {signal} (0x7) at pc=0x1" if signal else "# no fatal signal line"
    siginfo = (
        f"siginfo: si_signo: 7 ({signal}), si_code: 2 ({code}), si_addr: 0x1"
        if signal
        else ""
    )
    return f"""{fatal}
Time: {timestamp} elapsed time: 3.500000 seconds (0d 0h 0m 3s)
Java frames: (J=compiled Java code, j=interpreted, Vv=VM code)
J 1  {frame}()V io.questdb@7.3.10
v  ~StubRoutines::call_stub
{siginfo}
"""


def test_t7_classifier_counts_signals_frames_and_hs_err(tmp_path):
    files = {
        "crash+0.log": _jvm_log(
            "SIGBUS", "BUS_ADRERR", "io.questdb.jit.FiltersCompiler.callFunction"
        ),
        "crash+1.log": _jvm_log(
            "SIGSEGV", "SEGV_MAPERR", "io.questdb.std.Rosti.keyedIntCount"
        ),
        "crash+2.log": _jvm_log(
            None, None, "io.questdb.std.BytecodeAssembler.newInstance"
        ),
        "hs_err_pid+1.log": _jvm_log(
            "SIGBUS",
            "BUS_ADRERR",
            "io.questdb.cairo.wal.seq.TableTransactionLog.getCursor",
        ),
    }
    for name, contents in files.items():
        (tmp_path / name).write_text(contents, encoding="utf-8")

    records = classify_directory(tmp_path)

    assert [record.filename for record in records] == [
        "crash+0.log",
        "crash+1.log",
        "crash+2.log",
        "hs_err_pid+1.log",
    ]
    assert Counter(record.signal for record in records) == {
        "SIGBUS": 2,
        "SIGSEGV": 1,
        None: 1,
    }
    assert Counter(record.first_questdb_frame for record in records) == {
        "io.questdb.jit.FiltersCompiler.callFunction": 1,
        "io.questdb.std.Rosti.keyedIntCount": 1,
        "io.questdb.std.BytecodeAssembler.newInstance": 1,
        "io.questdb.cairo.wal.seq.TableTransactionLog.getCursor": 1,
    }
