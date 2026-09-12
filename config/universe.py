"""Versioned contract for the authoritative IDX Syariah universe."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AUTHORITATIVE_UNIVERSE_PATH = Path(__file__).with_name("syariah_stocks.txt")
UNIVERSE_MANIFEST_PATH = Path(__file__).with_name("universe.json")


class UniverseContractError(ValueError):
    """Raised when the authoritative universe disagrees with its manifest."""


def normalize_lf(content: bytes) -> bytes:
    """Return content with CRLF and lone-CR endings normalized to LF."""
    return content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def parse_symbols(content: bytes) -> list[str]:
    """Parse bare symbols from already-normalized universe content."""
    return [
        line.strip()
        for line in content.decode("utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def load_manifest(path: Path = UNIVERSE_MANIFEST_PATH) -> dict[str, Any]:
    """Load the versioned universe manifest."""
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def resolve_universe_path(path: str | Path) -> Path:
    """Resolve repository-relative universe paths the same way live flows do."""
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = REPOSITORY_ROOT / candidate
    return candidate.resolve()


def verify_authoritative_universe(
    list_path: str | Path = AUTHORITATIVE_UNIVERSE_PATH,
    manifest_path: Path = UNIVERSE_MANIFEST_PATH,
) -> list[str]:
    """Verify an authoritative-list candidate against the versioned manifest."""
    resolved = resolve_universe_path(list_path)
    normalized = normalize_lf(resolved.read_bytes())
    symbols = parse_symbols(normalized)
    manifest = load_manifest(manifest_path)
    actual_hash = hashlib.sha256(normalized).hexdigest()

    if actual_hash != manifest.get("sha256_lf"):
        raise UniverseContractError(
            f"Universe hash mismatch for {resolved}: "
            f"expected {manifest.get('sha256_lf')}, got {actual_hash}"
        )
    if len(symbols) != manifest.get("symbol_count"):
        raise UniverseContractError(
            f"Universe count mismatch for {resolved}: "
            f"expected {manifest.get('symbol_count')}, got {len(symbols)}"
        )
    return symbols


def load_universe(path: str | Path) -> list[str]:
    """Load a universe, verifying only the authoritative path (D3)."""
    resolved = resolve_universe_path(path)
    if resolved == AUTHORITATIVE_UNIVERSE_PATH.resolve():
        return verify_authoritative_universe(resolved)

    logger.warning(
        "NON-AUTHORITATIVE UNIVERSE: custom list %s is not verified",
        resolved,
    )
    return parse_symbols(normalize_lf(resolved.read_bytes()))
