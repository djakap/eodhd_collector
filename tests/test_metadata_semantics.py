import ast
from datetime import datetime
import inspect
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest

from collectors.yfinance_price_collector import YFinancePriceCollector
from config import tables
from db.questdb_client import (
    QuestDBClient,
    _metadata_table_for_price_table,
)


ROOT = Path(__file__).resolve().parents[1]


def _python_files():
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.py"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return sorted(set(result.stdout.splitlines()))


def _sample_records():
    return [
        {
            "symbol": "TEST.JK",
            "interval": "d",
            "timestamp": datetime(2026, 1, day),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "adjusted_close": 1.0,
            "volume": 1,
            "source": "fixture",
        }
        for day in (1, 2, 3)
    ]


class RecordingDB:
    def __init__(self, metadata_error=None):
        self.price_writes = []
        self.metadata_writes = []
        self.metadata_error = metadata_error

    def insert_price_data(self, records, table):
        self.price_writes.append((records, table))

    def upsert_stock_metadata(self, *args, **kwargs):
        if self.metadata_error:
            raise self.metadata_error
        self.metadata_writes.append((args, kwargs))


def _collector(target_table, db=None):
    collector = object.__new__(YFinancePriceCollector)
    collector.target_table = target_table
    collector.db = db or RecordingDB()
    return collector


class SequencedCursor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))

    def fetchone(self):
        return self.responses.pop(0)


def test_u0_metadata_authority_names_are_explicit_and_old_name_is_removed():
    assert tables.TABLE_PRICE_METADATA_PRODUCTION == "stock_metadata"
    assert tables.TABLE_PRICE_METADATA_LEGACY_EODHD == "eodhd_stock_metadata"
    assert (
        tables.TABLE_PRICE_METADATA_PRODUCTION
        != tables.TABLE_PRICE_METADATA_LEGACY_EODHD
    )

    from config import db_config

    assert not hasattr(db_config, "TABLE_" + "STOCK_METADATA")
    with pytest.raises(ImportError):
        exec("from config.db_config import TABLE_" + "STOCK_METADATA", {})


def test_u1_batch_coverage_cannot_be_passed_to_metadata_writer():
    assert list(inspect.signature(QuestDBClient.upsert_stock_metadata).parameters) == [
        "self",
        "symbol",
        "interval",
        "table",
    ]


def test_u2_price_to_metadata_mapping_is_total_and_unknown_tables_raise():
    assert (
        _metadata_table_for_price_table(tables.TABLE_PRICES_PRODUCTION)
        == tables.TABLE_PRICE_METADATA_PRODUCTION
    )
    assert (
        _metadata_table_for_price_table(tables.TABLE_PRICES_LEGACY_EODHD)
        == tables.TABLE_PRICE_METADATA_LEGACY_EODHD
    )
    with pytest.raises(ValueError, match="No metadata table"):
        _metadata_table_for_price_table("unrecognised_price_table")


def test_u3_u4_real_collection_path_persists_matching_metadata_authority():
    for price_table in (
        tables.TABLE_PRICES_PRODUCTION,
        tables.TABLE_PRICES_LEGACY_EODHD,
    ):
        collector = _collector(price_table)
        assert collector._store("TEST.JK", "d", _sample_records()) == 3
        assert collector.db.metadata_writes == [
            (("TEST.JK", "d"), {"table": price_table})
        ]

    source = (ROOT / "collectors/yfinance_price_collector.py").read_text()
    store = next(
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name == "_store"
    )
    comparisons = [node for node in ast.walk(store) if isinstance(node, ast.Compare)]
    assert any(
        isinstance(node.left, ast.Attribute)
        and node.left.attr == "target_table"
        and any(
            isinstance(comparator, ast.Name)
            and comparator.id == "TABLE_PRICES_PRODUCTION"
            for comparator in node.comparators
        )
        for node in comparisons
    )


def test_u5_writer_derives_synthetic_coverage_from_the_price_table():
    data_start = datetime(1970, 1, 2)
    data_end = datetime(2099, 12, 31)
    cursor = SequencedCursor([(999111, data_start, data_end), None])
    client = object.__new__(QuestDBClient)
    client.cursor = cursor
    client.ensure_connection = lambda: None

    client.upsert_stock_metadata(
        "TEST.JK", "d", table=tables.TABLE_PRICES_PRODUCTION
    )

    coverage_sql, coverage_parameters = cursor.calls[0]
    assert 'FROM "stock_data"' in coverage_sql
    assert coverage_parameters == ("TEST.JK", "d")

    insert_sql, insert_parameters = cursor.calls[-1]
    assert "INSERT INTO stock_metadata" in insert_sql
    assert insert_parameters[3:6] == (999111, data_start, data_end)


def test_u6_no_call_site_can_pass_batch_coverage_values():
    violations = []
    removed_keywords = {"total_records", "data_start", "data_end"}
    for path in _python_files():
        if path.startswith("archive/2026-02-flat-layout/"):
            continue
        tree = ast.parse((ROOT / path).read_text(), filename=path)
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "upsert_stock_metadata"
            ):
                continue
            bad_keywords = sorted(
                keyword.arg for keyword in node.keywords if keyword.arg in removed_keywords
            )
            if bad_keywords or len(node.args) > 3:
                violations.append((path, node.lineno, bad_keywords, len(node.args)))
    assert violations == []


def test_u9_metadata_failures_remain_non_fatal():
    collector = _collector(
        tables.TABLE_PRICES_PRODUCTION,
        RecordingDB(metadata_error=RuntimeError("metadata unavailable")),
    )
    assert collector._store("TEST.JK", "d", _sample_records()) == 3
    assert len(collector.db.price_writes) == 1

    class RaisingCursor:
        def execute(self, statement, parameters=None):
            raise RuntimeError("coverage unavailable")

    client = object.__new__(QuestDBClient)
    client.cursor = RaisingCursor()
    client.ensure_connection = lambda: None
    assert (
        client.upsert_stock_metadata(
            "TEST.JK", "d", table=tables.TABLE_PRICES_PRODUCTION
        )
        is None
    )


def test_u10_metadata_coverage_reader_set_is_explicit():
    readers = set()
    field_pattern = re.compile(r"\b(?:total_records|data_start)\b")
    for path in _python_files():
        if path.startswith(("archive/2026-02-flat-layout/", "tests/")):
            continue
        tree = ast.parse((ROOT / path).read_text(), filename=path)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            is_query = (
                isinstance(node.func, ast.Attribute) and node.func.attr == "execute"
            ) or (isinstance(node.func, ast.Name) and node.func.id == "http_query")
            if not is_query:
                continue
            query_shape = ast.unparse(node.args[0]).lower()
            if (
                "select" in query_shape
                and ("metadata" in query_shape or "table_meta" in query_shape)
                and field_pattern.search(query_shape)
            ):
                readers.add(path)

    assert readers == {
        "flows/data_quality_flow.py",
        "scripts/legacy_eodhd/eodhd_data_validator.py",
    }


def test_u13_renamed_importers_resolve_same_legacy_value_without_database():
    env = os.environ.copy()
    env["QUESTDB_PG_PORT"] = "1"
    command = (
        "import db.questdb_client as q; "
        "import flows.data_quality_flow as f; "
        "print(q.TABLE_PRICE_METADATA_LEGACY_EODHD); "
        "print(f.TABLE_PRICE_METADATA_LEGACY_EODHD)"
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )
    assert result.stdout.splitlines() == [
        "eodhd_stock_metadata",
        "eodhd_stock_metadata",
    ]
