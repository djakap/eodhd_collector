import ast
import importlib
import inspect
from pathlib import Path
import subprocess
import sys

from collectors.yfinance_price_collector import YFinancePriceCollector
from config.tables import (
    TABLE_ACTIONS_LEGACY_EODHD,
    TABLE_ACTIONS_PRODUCTION,
    TABLE_PRICES_LEGACY_EODHD,
    TABLE_PRICES_PRODUCTION,
)


ROOT = Path(__file__).resolve().parents[1]


def _source(path):
    return (ROOT / path).read_text()


def _table_expressions(path, method_name):
    expressions = []
    for node in ast.walk(ast.parse(_source(path), filename=path)):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == method_name
        ):
            continue
        keyword = next((item.value for item in node.keywords if item.arg == "table"), None)
        value = keyword if keyword is not None else node.args[1] if len(node.args) >= 2 else None
        expressions.append(ast.unparse(value) if value is not None else None)
    return expressions


def _assigned_string(path, name):
    tree = ast.parse(_source(path), filename=path)
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(target, ast.Name) and target.id == name for target in targets):
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return node.value.value
    raise AssertionError(f"{name} is not a direct string assignment in {path}")


def test_t5_every_legacy_caller_preserves_its_table_mapping():
    expected = {
        "collectors/bulk_collector.py": {
            "insert_price_data": ["TABLE_PRICES_LEGACY_EODHD"],
            "insert_corporate_actions": [
                "TABLE_ACTIONS_LEGACY_EODHD",
                "TABLE_ACTIONS_LEGACY_EODHD",
            ],
        },
        "collectors/action_collector.py": {
            "insert_corporate_actions": [
                "TABLE_ACTIONS_LEGACY_EODHD",
                "TABLE_ACTIONS_LEGACY_EODHD",
            ],
        },
        "collectors/price_collector.py": {
            "get_existing_timestamps": ["TABLE_PRICES_LEGACY_EODHD"] * 4,
            "insert_price_data": ["TABLE_PRICES_LEGACY_EODHD"] * 2,
        },
        "scripts/backfill_nov2024.py": {
            "insert_price_data": ["TABLE_PRICES_LEGACY_EODHD"] * 2,
        },
        "scripts/fill_june_gap.py": {
            "insert_price_data": ["TABLE_PRICES_LEGACY_EODHD"],
        },
        "scripts/fill_missing_periods.py": {
            "insert_price_data": ["TABLE_PRICES_LEGACY_EODHD"],
        },
        "scripts/legacy_eodhd/backfill_gap.py": {
            "insert_price_data": ["TABLE_PRICES_LEGACY_EODHD"],
        },
        "scripts/legacy_eodhd/backfill_nov2024.py": {
            "insert_price_data": ["TABLE_PRICES_LEGACY_EODHD"] * 2,
        },
    }
    for path, methods in expected.items():
        for method, expressions in methods.items():
            assert _table_expressions(path, method) == expressions

    assert TABLE_PRICES_LEGACY_EODHD == "eodhd_stock_data"
    assert TABLE_ACTIONS_LEGACY_EODHD == "eodhd_corporate_actions"


def test_t5_production_callers_and_migrations_preserve_their_targets():
    init = inspect.signature(YFinancePriceCollector).parameters
    assert init["target_table"].default == TABLE_PRICES_PRODUCTION
    assert init["actions_table"].default == TABLE_ACTIONS_PRODUCTION
    assert _table_expressions(
        "collectors/yfinance_price_collector.py", "insert_price_data"
    ) == ["self.target_table"]
    assert _table_expressions(
        "collectors/yfinance_price_collector.py", "insert_corporate_actions"
    ) == ["self.actions_table"]

    intraday = importlib.import_module("flows.intraday_flow")
    assert intraday.PROD == TABLE_PRICES_PRODUCTION
    assert _table_expressions("flows/intraday_flow.py", "insert_price_data") == ["PROD"]

    migration_targets = {
        "scripts/promote_to_production.py": "stock_data",
        "scripts/dedup_periods.py": "eodhd_stock_data_dd",
        "scripts/rebuild_stock_data.py": "eodhd_stock_data_v2",
    }
    for path, expected in migration_targets.items():
        assert _assigned_string(path, "NEW") == expected
        assert _table_expressions(path, "insert_price_data") == ["NEW"]


def test_t7_all_touched_and_caller_modules_import_in_one_fresh_interpreter():
    modules = [
        "config.tables",
        "config.db_config",
        "config.yfinance_config",
        "db.questdb_client",
        "flows.data_quality_flow",
        "utils.aggregate_4h",
        "collectors.yfinance_price_collector",
        "scripts.fill_missing_periods",
        "scripts.compare_sources",
        "scripts.fill_june_gap",
        "scripts.adjudicate_out_of_session",
        "scripts.reconcile_periods",
        "collectors.bulk_collector",
        "collectors.action_collector",
        "collectors.price_collector",
        "scripts.backfill_nov2024",
        "scripts.legacy_eodhd.backfill_gap",
        "scripts.legacy_eodhd.backfill_nov2024",
        "flows.intraday_flow",
        "flows.yfinance_price_flow",
    ]
    command = "import importlib; " + "; ".join(
        f"importlib.import_module({module!r})" for module in modules
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    # These migration tools connect to QuestDB at import time. Parse them without
    # executing module-level runtime code so this regression remains portable.
    for path in (
        "scripts/promote_to_production.py",
        "scripts/dedup_periods.py",
        "scripts/rebuild_stock_data.py",
    ):
        ast.parse(_source(path), filename=path)


def _working_python_files():
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.py"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return sorted(set(result.stdout.splitlines()))


def test_t8_no_insert_call_omits_a_table_argument():
    methods = {"insert_price_data", "insert_corporate_actions"}
    omissions = []
    for path in _working_python_files():
        if path.startswith("archive/2026-02-flat-layout/"):
            continue
        for node in ast.walk(ast.parse(_source(path), filename=path)):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in methods
            ):
                continue
            has_table = len(node.args) >= 2 or any(
                keyword.arg == "table" for keyword in node.keywords
            )
            if not has_table:
                omissions.append((path, node.lineno, node.func.attr))
    assert omissions == []


def test_t9_removed_names_are_absent_outside_the_frozen_archive():
    removed_names = [
        "TABLE_" + "STOCK_DATA",
        "TABLE_" + "STOCK_METADATA",
        "TABLE_" + "CORPORATE_ACTIONS",
        "TABLE_YF_" + "STOCK_DATA",
        "TABLE_CORPORATE_" + "ACTIONS_YF",
    ]
    occurrences = []
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    for path in sorted(set(result.stdout.splitlines())):
        if path.startswith("archive/2026-02-flat-layout/"):
            continue
        data = (ROOT / path).read_bytes()
        for name in removed_names:
            if name.encode() in data:
                occurrences.append((path, name))
    assert occurrences == []
