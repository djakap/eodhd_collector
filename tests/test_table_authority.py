import ast
from collections import Counter
import importlib
import inspect
import os
from pathlib import Path
import subprocess
import sys

import pytest

from collectors.yfinance_price_collector import YFinancePriceCollector
from config import tables
from db.questdb_client import QuestDBClient


ROOT = Path(__file__).resolve().parents[1]
BASE_COMMIT = "20e492a1e3f55b5ef2eb12a62462518f7a6ef9d6"


def _run(*args):
    return subprocess.run(
        args,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout


def test_t0_authority_module_defines_exact_names_and_values():
    expected = {
        "TABLE_PRICES_PRODUCTION": "stock_data",
        "TABLE_PRICES_LEGACY_EODHD": "eodhd_stock_data",
        "TABLE_ACTIONS_PRODUCTION": "corporate_actions",
        "TABLE_ACTIONS_LEGACY_EODHD": "eodhd_corporate_actions",
    }
    actual = {
        name: value
        for name, value in vars(tables).items()
        if name.startswith("TABLE_")
    }
    assert actual == expected


def test_t1_production_and_legacy_pairs_are_distinct():
    assert tables.TABLE_PRICES_PRODUCTION != tables.TABLE_PRICES_LEGACY_EODHD
    assert tables.TABLE_ACTIONS_PRODUCTION != tables.TABLE_ACTIONS_LEGACY_EODHD


@pytest.mark.parametrize(
    ("module_name", "removed_name"),
    [
        ("config.db_config", "TABLE_" + "STOCK_DATA"),
        ("config.db_config", "TABLE_" + "CORPORATE_ACTIONS"),
        ("config.yfinance_config", "TABLE_YF_" + "STOCK_DATA"),
        ("config.yfinance_config", "TABLE_CORPORATE_" + "ACTIONS_YF"),
    ],
)
def test_t2_ambiguous_names_cannot_be_imported(module_name, removed_name):
    module = importlib.import_module(module_name)
    assert not hasattr(module, removed_name)
    with pytest.raises(ImportError):
        exec(f"from {module_name} import {removed_name}", {})


def _price_table_from_fresh_interpreter(env_value):
    env = os.environ.copy()
    if env_value is None:
        env.pop("YF_PRICE_TABLE", None)
    else:
        env["YF_PRICE_TABLE"] = env_value
    command = (
        "import dotenv; dotenv.load_dotenv = lambda: None; "
        "from config.yfinance_config import YF_PRICE_TABLE; "
        "print(YF_PRICE_TABLE)"
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()


def test_t3_price_target_defaults_to_production_and_honours_environment():
    assert _price_table_from_fresh_interpreter(None) == tables.TABLE_PRICES_PRODUCTION
    assert _price_table_from_fresh_interpreter("comparison_prices") == "comparison_prices"


@pytest.mark.parametrize(
    "method_name",
    [
        "insert_price_data",
        "_insert_price_data_ilp",
        "_insert_price_data_sql",
        "insert_corporate_actions",
        "get_existing_timestamps",
    ],
)
def test_t4_table_parameter_is_required(method_name):
    parameter = inspect.signature(getattr(QuestDBClient, method_name)).parameters["table"]
    assert parameter.default is inspect.Parameter.empty


def test_t4_omitting_price_table_fails_before_method_body():
    client = object.__new__(QuestDBClient)
    insert_without_table = getattr(client, "insert_" + "price_data")
    with pytest.raises(TypeError):
        insert_without_table([])


class RecordingDB:
    def __init__(self):
        self.price_writes = []
        self.metadata_writes = []

    def insert_price_data(self, records, table):
        self.price_writes.append((records, table))

    def upsert_stock_metadata(self, *args, **kwargs):
        self.metadata_writes.append((args, kwargs))


def _collector_with_fake_db(target_table):
    collector = object.__new__(YFinancePriceCollector)
    collector.target_table = target_table
    collector.db = RecordingDB()
    return collector


def _sample_price_record():
    return {
        "symbol": "TEST.JK",
        "interval": "d",
        "timestamp": __import__("datetime").datetime(2026, 9, 10),
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "adjusted_close": 1.0,
        "volume": 1,
        "source": "fixture",
    }


def test_t6_metadata_guard_structurally_compares_with_legacy_price_table():
    source = (ROOT / "collectors/yfinance_price_collector.py").read_text()
    tree = ast.parse(source)
    store = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_store"
    )
    comparisons = [node for node in ast.walk(store) if isinstance(node, ast.Compare)]
    assert any(
        isinstance(node.left, ast.Attribute)
        and node.left.attr == "target_table"
        and any(
            isinstance(comparator, ast.Name)
            and comparator.id == "TABLE_PRICES_LEGACY_EODHD"
            for comparator in node.comparators
        )
        for node in comparisons
    )
    assert tables.TABLE_PRICES_LEGACY_EODHD != tables.TABLE_PRICES_PRODUCTION


def test_t6_metadata_guard_stays_dead_for_production_and_live_for_legacy():
    production = _collector_with_fake_db(tables.TABLE_PRICES_PRODUCTION)
    assert production._store("TEST.JK", "d", [_sample_price_record()]) == 1
    assert production.db.metadata_writes == []

    legacy = _collector_with_fake_db(tables.TABLE_PRICES_LEGACY_EODHD)
    assert legacy._store("TEST.JK", "d", [_sample_price_record()]) == 1
    assert len(legacy.db.metadata_writes) == 1


def test_t10_archive_is_byte_identical_and_retains_historical_names():
    archive = "archive/2026-02-flat-layout"
    status = _run("git", "status", "--porcelain=v1", "--", archive)
    assert status == ""
    subprocess.run(
        ["git", "diff", "--quiet", BASE_COMMIT, "--", archive],
        cwd=ROOT,
        check=True,
    )
    historical_name = "TABLE_" + "STOCK_DATA"
    references = 0
    for path in (ROOT / archive).rglob("*"):
        if path.is_file():
            references += path.read_bytes().count(historical_name.encode())
    assert references == 13


def _direct_table_literals(ref=None):
    if ref:
        paths = _run("git", "ls-tree", "-r", "--name-only", ref, "--", "config").splitlines()
    else:
        paths = [str(path.relative_to(ROOT)) for path in (ROOT / "config").glob("*.py")]

    values = []
    for path in paths:
        if not path.endswith(".py"):
            continue
        source = _run("git", "show", f"{ref}:{path}") if ref else (ROOT / path).read_text()
        for node in ast.walk(ast.parse(source, filename=path)):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names = [target.id for target in targets if isinstance(target, ast.Name)]
            if (
                any(name.startswith("TABLE_") for name in names)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            ):
                values.append(node.value.value)
    return Counter(values)


def test_t11_direct_config_table_literal_multiset_is_unchanged():
    assert _direct_table_literals() == _direct_table_literals(BASE_COMMIT)
