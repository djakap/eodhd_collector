"""Regression tests for QCF-003 nightly completeness semantics."""

from datetime import date, datetime
import logging

import pytest

import flows.nightly_check_flow as nightly
from flows.fundamentals_flow import load_symbols


SETTLED = datetime(2026, 9, 25)
OLDER = datetime(2026, 9, 24)
AS_OF = date(2026, 9, 26)
RETIRED = {"INSA.JK", "JASS.JK", "RINA.JK", "SIMM.JK", "SING.JK", "SQBB.JK"}


def _symbols(count=651):
    return [f"S{index:03d}.JK" for index in range(count)]


def _state(symbols, observations, as_of=AS_OF):
    return nightly._compute_completeness(symbols, observations, as_of_date=as_of)


def test_t0_all_current_has_full_coverage():
    symbols = _symbols()
    result = _state(symbols, {symbol: SETTLED for symbol in symbols})

    assert result == {
        "settled": "2026-09-25",
        "total": 651,
        "coverage": 651,
        "coverage_ratio": 1.0,
        "laggards": [],
        "missing_entirely": [],
        "stale_days": 1,
    }


def test_t1_stale_symbols_are_separate_and_lower_coverage():
    symbols = _symbols()
    stale = set(symbols[-5:])
    observations = {
        symbol: OLDER if symbol in stale else SETTLED
        for symbol in symbols
    }

    result = _state(symbols, observations)

    assert set(result["laggards"]) == stale
    assert result["missing_entirely"] == []
    assert result["coverage"] == 646
    assert result["coverage_ratio"] == pytest.approx(646 / 651)


def test_t2_zero_row_symbol_is_missing_and_not_covered():
    symbols = _symbols()
    observations = {symbol: SETTLED for symbol in symbols[:-1]}

    result = _state(symbols, observations)

    assert result["missing_entirely"] == [symbols[-1]]
    assert result["laggards"] == []
    assert result["coverage"] == 650
    assert result["coverage_ratio"] == pytest.approx(650 / 651)


def test_t3_current_stale_and_missing_partition_the_universe():
    symbols = _symbols()
    missing = set(symbols[-6:])
    stale = set(symbols[-11:-6])
    observations = {
        symbol: OLDER if symbol in stale else SETTLED
        for symbol in symbols
        if symbol not in missing
    }

    result = _state(symbols, observations)
    current = set(symbols) - set(result["laggards"]) - set(result["missing_entirely"])

    assert set(result["laggards"]) == stale
    assert set(result["missing_entirely"]) == missing
    assert current.isdisjoint(stale)
    assert current.isdisjoint(missing)
    assert stale.isdisjoint(missing)
    assert current | stale | missing == set(symbols)
    assert result["coverage"] == len(current)


def test_t4_ratio_and_total_are_present_even_without_a_settled_day():
    symbols = _symbols()
    observations = {symbol: SETTLED for symbol in symbols[:585]}

    result = _state(symbols, observations)

    assert result["settled"] is None
    assert result["total"] == 651
    assert result["coverage"] == 585
    assert result["coverage_ratio"] == pytest.approx(result["coverage"] / result["total"])


def test_t5_sixty_five_zero_row_symbols_are_just_below_the_cliff():
    symbols = _symbols()
    observations = {symbol: SETTLED for symbol in symbols[:586]}

    result = _state(symbols, observations)

    assert result["settled"] == "2026-09-25"
    assert len(result["missing_entirely"]) == 65
    assert result["coverage"] == 586


def _cliff_state(*, with_stale=False):
    symbols = _symbols()
    observations = {symbol: SETTLED for symbol in symbols[:585]}
    if with_stale:
        observations[symbols[584]] = OLDER
    return symbols, _state(symbols, observations)


def test_t6_sixty_six_zero_row_symbols_keep_full_diagnostics():
    symbols, result = _cliff_state(with_stale=True)

    assert result["settled"] is None
    assert len(result["missing_entirely"]) == 66
    assert result["laggards"] == [symbols[584]]
    assert result["total"] == 651
    assert result["coverage"] == 584
    assert result != {"settled": None, "laggards": [], "coverage": 0}


def test_t7_cliff_verdict_names_missing_count_without_key_error(monkeypatch):
    _, result = _cliff_state(with_stale=True)
    received_candidates = []

    def fake_heal(candidates):
        received_candidates.extend(candidates)
        return {"attempted": len(candidates), "healed": 0}

    monkeypatch.setattr(nightly, "get_run_logger", lambda: logging.getLogger("qcf003-t7"))
    monkeypatch.setattr(nightly, "self_heal_wal", lambda: 0)
    monkeypatch.setattr(nightly, "check_partition_readability", lambda: 0)
    monkeypatch.setattr(nightly, "check_adjustment_consistency", lambda: 0)
    monkeypatch.setattr(nightly, "quality", lambda _table: {})
    monkeypatch.setattr(nightly, "check_completeness", lambda _path: result)
    monkeypatch.setattr(nightly, "heal", fake_heal)

    with pytest.raises(RuntimeError, match="66 simbol tanpa data") as exc_info:
        nightly.nightly_check_flow.fn("synthetic")

    assert "KeyError" not in str(exc_info.value)
    assert received_candidates == sorted(result["laggards"] + result["missing_entirely"])


def test_t8_more_than_heal_cap_is_systemic_without_constructing_collector(monkeypatch):
    def forbidden_collector(*_args, **_kwargs):
        raise AssertionError("collector must not be constructed above HEAL_CAP")

    monkeypatch.setattr(nightly, "get_run_logger", lambda: logging.getLogger("qcf003-t8"))
    monkeypatch.setattr(nightly, "YFinancePriceCollector", forbidden_collector)

    result = nightly.heal.fn(_symbols(nightly.HEAL_CAP + 1))

    assert result == {"attempted": 0, "healed": 0, "systemic": True}


def test_t9_weekend_and_preclose_clock_do_not_advance_settled_day():
    symbols = _symbols()
    observations = {symbol: SETTLED for symbol in symbols}

    friday = _state(symbols, observations, date(2026, 9, 25))
    weekend = _state(symbols, observations, date(2026, 9, 27))
    monday_preclose = _state(symbols, observations, date(2026, 9, 28))

    assert friday["settled"] == weekend["settled"] == monday_preclose["settled"] == "2026-09-25"
    assert friday["coverage"] == weekend["coverage"] == monday_preclose["coverage"] == 651
    assert [friday["stale_days"], weekend["stale_days"], monday_preclose["stale_days"]] == [0, 2, 3]


def test_t10_synthetic_zero_row_is_missing_and_retired_symbols_stay_retired():
    authoritative = set(load_symbols("config/syariah_stocks.txt", None))
    synthetic = "SYNTHETIC.JK"
    universe = authoritative | {synthetic}
    observations = {symbol: SETTLED for symbol in authoritative}

    result = _state(universe, observations)

    assert result["missing_entirely"] == [synthetic]
    assert RETIRED.isdisjoint(authoritative)


def test_t12_sixty_six_candidates_are_fetched_below_the_cap(monkeypatch):
    calls = []

    class FakeCollector:
        def __init__(self, update_mode):
            assert update_mode is True

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def collect_all(self, symbol, skip_intraday, skip_actions):
            assert skip_intraday is False
            assert skip_actions is True
            calls.append(symbol)
            return {"eod": False, "intraday": False}

    _, cliff = _cliff_state()
    candidates = sorted(set(cliff["laggards"]) | set(cliff["missing_entirely"]))
    assert len(candidates) == 66
    monkeypatch.setattr(nightly, "get_run_logger", lambda: logging.getLogger("qcf003-t12"))
    monkeypatch.setattr(nightly, "YFinancePriceCollector", FakeCollector)

    result = nightly.heal.fn(candidates)

    assert result == {"attempted": 66, "healed": 0}
    assert calls == candidates
