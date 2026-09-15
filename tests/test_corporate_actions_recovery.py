import ast
from pathlib import Path

import pytest

from scripts.check_table_partitions import assess_partition_state
from scripts.restore_production_corporate_actions import (
    MODE_DRY_RUN,
    parse_args,
    validate_empty_build_target,
)


ROOT = Path(__file__).resolve().parents[1]


def test_t0_partition_detector_flags_the_incident_signature():
    result = assess_partition_state(
        "corporate_actions",
        [("2000", 7), ("2001", 0), ("2002", 3819)],
        readable_rows=27,
        suspended=False,
    )

    assert result.partition_rows == 3826
    assert result.readable_rows == 27
    assert result.problem is True
    assert "3826" in result.message
    assert "27" in result.message


def test_t1_partition_detector_accepts_matching_inventory():
    result = assess_partition_state(
        "corporate_actions",
        [("2000", 7), ("2001", 20)],
        readable_rows=27,
        suspended=False,
    )

    assert result.problem is False
    assert result.deferred is False
    assert result.message == "corporate_actions: healthy (27 rows across 2 partitions)"


def test_t2_suspended_wal_defers_without_double_reporting():
    result = assess_partition_state(
        "corporate_actions",
        [("2000", 3826)],
        readable_rows=27,
        suspended=True,
    )

    assert result.problem is False
    assert result.deferred is True
    assert "existing WAL heal path" in result.message


def test_t3_detector_contains_only_read_queries_and_no_repair_path():
    path = ROOT / "scripts/check_table_partitions.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    executed_sql = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute"
            and node.args
        ):
            continue
        argument = node.args[0]
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
            executed_sql.append(argument.value.strip().upper())
        elif isinstance(argument, ast.JoinedStr):
            prefix = "".join(
                value.value
                for value in argument.values
                if isinstance(value, ast.Constant) and isinstance(value.value, str)
            ).strip().upper()
            executed_sql.append(prefix)

    assert executed_sql
    assert all(statement.startswith("SELECT") for statement in executed_sql)


def test_t4_restore_is_dry_run_first_and_refuses_nonempty_target():
    assert parse_args(["--source-port", "18812"]).mode == MODE_DRY_RUN
    validate_empty_build_target(exists=False, row_count=None)
    validate_empty_build_target(exists=True, row_count=0)
    with pytest.raises(RuntimeError, match="non-empty"):
        validate_empty_build_target(exists=True, row_count=1)
