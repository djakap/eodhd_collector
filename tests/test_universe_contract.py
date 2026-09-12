"""Regression coverage for the authoritative-universe contract."""

import ast
import fnmatch
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from config.universe import (
    AUTHORITATIVE_UNIVERSE_PATH,
    UNIVERSE_MANIFEST_PATH,
    UniverseContractError,
    load_universe,
    normalize_lf,
    parse_symbols,
    verify_authoritative_universe,
)
from flows.fundamentals_flow import load_symbols


ROOT = Path(__file__).resolve().parents[1]


def test_t0_manifest_matches_authoritative_list():
    manifest = json.loads(UNIVERSE_MANIFEST_PATH.read_text(encoding="utf-8"))
    normalized = normalize_lf(AUTHORITATIVE_UNIVERSE_PATH.read_bytes())

    assert manifest["symbol_count"] == 651
    assert len(parse_symbols(normalized)) == manifest["symbol_count"]
    assert hashlib.sha256(normalized).hexdigest() == manifest["sha256_lf"]
    assert verify_authoritative_universe() == parse_symbols(normalized)


@pytest.mark.parametrize("mutation", ["drop", "add", "change"])
def test_t1_verifier_rejects_divergent_content(tmp_path, mutation):
    lines = normalize_lf(AUTHORITATIVE_UNIVERSE_PATH.read_bytes()).splitlines()
    if mutation == "drop":
        lines = lines[:-1]
    elif mutation == "add":
        lines.append(b"ZZZZ")
    else:
        lines[0] = b"X" + lines[0][1:]
    candidate = tmp_path / "syariah_stocks.txt"
    candidate.write_bytes(b"\n".join(lines) + b"\n")

    with pytest.raises(UniverseContractError):
        verify_authoritative_universe(candidate)


def test_t3_lf_normalization_accepts_crlf_copy(tmp_path):
    normalized = normalize_lf(AUTHORITATIVE_UNIVERSE_PATH.read_bytes())
    candidate = tmp_path / "syariah_stocks.txt"
    candidate.write_bytes(normalized.replace(b"\n", b"\r\n"))

    assert len(verify_authoritative_universe(candidate)) == 651


def test_d3_custom_collector_path_is_loaded_but_labelled(tmp_path, caplog):
    candidate = tmp_path / "custom.txt"
    candidate.write_text("BANK\nBRIS\n", encoding="utf-8")

    with caplog.at_level("WARNING"):
        assert load_universe(candidate) == ["BANK", "BRIS"]
        assert load_symbols(str(candidate)) == ["BANK.JK", "BRIS.JK"]

    notices = [record.message for record in caplog.records if "NON-AUTHORITATIVE" in record.message]
    assert len(notices) == 2
    assert all(str(candidate) in notice for notice in notices)


def _tracked_python_files(*roots: str) -> list[Path]:
    output = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.py"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.splitlines()
    return [
        ROOT / path
        for path in output
        if any(path.startswith(f"{root}/") for root in roots)
    ]


def test_t6_all_collector_loader_implementations_use_the_authoritative_default():
    expected = {
        ("flows/daily_collection_flow.py", "load_stocks"),
        ("flows/fundamentals_flow.py", "load_symbols"),
        ("scripts/fill_june_gap.py", "load_symbols"),
        ("scripts/reconcile_periods.py", "load_symbols"),
        ("scripts/legacy_eodhd/main_ultrafast.py", "load_stocks"),
        ("scripts/legacy_eodhd/run_screener_refresh.py", "load_stocks"),
    }
    actual = set()
    universe_literals = []
    for path in _tracked_python_files("flows", "scripts"):
        relative = str(path.relative_to(ROOT))
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        definitions = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name in {"load_stocks", "load_symbols"}
        ]
        universe_literals.extend(
            (relative, node.value)
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.endswith("syariah_stocks.txt")
        )
        actual.update((relative, name) for name in definitions)

    assert actual == expected
    assert universe_literals
    assert [
        item for item in universe_literals if item[1] != "config/syariah_stocks.txt"
    ] == []


def test_t7_nightly_completeness_reaches_the_verifier_structurally():
    fundamentals = ast.parse((ROOT / "flows/fundamentals_flow.py").read_text())
    loader = next(
        node
        for node in ast.walk(fundamentals)
        if isinstance(node, ast.FunctionDef) and node.name == "load_symbols"
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "load_universe"
        for node in ast.walk(loader)
    )

    nightly = ast.parse((ROOT / "flows/nightly_check_flow.py").read_text())
    completeness = next(
        node
        for node in ast.walk(nightly)
        if isinstance(node, ast.FunctionDef) and node.name == "check_completeness"
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "load_symbols"
        for node in ast.walk(completeness)
    )


def test_r5_universe_contract_is_in_the_static_worker_build_context():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY config/ config/" in dockerfile

    patterns = [
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#") and not line.startswith("!")
    ]
    candidates = ("config/universe.py", "config/universe.json")
    excluded = []
    for candidate in candidates:
        for pattern in patterns:
            directory_pattern = pattern.rstrip("/") + "/"
            if fnmatch.fnmatch(candidate, pattern) or candidate.startswith(directory_pattern):
                excluded.append((candidate, pattern))
    assert excluded == []

    for candidate in candidates:
        subprocess.run(
            ["git", "ls-files", "--error-unmatch", candidate],
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
