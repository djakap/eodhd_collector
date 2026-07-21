"""
Prefect Flow: yfinance fundamentals & valuation collection

Two entry points with deliberately different cadences:

  fundamentals_flow  — weekly. Financial statements only change quarterly, and a
                       full sweep is the heaviest thing we ask of yfinance
                       (6 statement calls per symbol). Running it daily would
                       burn rate-limit budget for data that has not moved.

  valuation_flow     — daily. One info call per symbol. This is the table that
                       accumulates what yfinance discards: marketCap, PE, PBV and
                       analyst targets are point-in-time and unrecoverable once
                       they change.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime
from typing import Dict, List, Optional

from prefect import flow, task, get_run_logger

from api.yfinance_client import YFinanceClient, RateLimitedError
from db.yfinance_writer import YFinanceWriter
from config.yfinance_config import YF_CIRCUIT_BREAKER

logger = logging.getLogger(__name__)


def load_symbols(stocks_file: str, limit: Optional[int] = None) -> List[str]:
    """
    Read the tracked stock list, normalised to Yahoo's '.JK' convention.

    config/syariah_stocks.txt stores bare codes ('BANK'), while Yahoo and the
    metadata table both use the suffixed form ('BANK.JK').
    """
    path = stocks_file
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), path)

    with open(path) as f:
        raw = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    symbols = [s if s.endswith('.JK') else f"{s}.JK" for s in raw]
    return symbols[:limit] if limit else symbols


# ---------------------------------------------------------------------------
# Task — fundamentals sweep
# ---------------------------------------------------------------------------

@task(name="Collect fundamentals", retries=1, retry_delay_seconds=300)
def collect_fundamentals(symbols: List[str]) -> Dict:
    log = get_run_logger()
    client = YFinanceClient()

    total_rows = 0
    with_data = 0
    without_data: List[str] = []
    consecutive_failures = 0
    aborted = False

    with YFinanceWriter() as writer:
        for i, symbol in enumerate(symbols, 1):
            try:
                rows = client.get_fundamentals(symbol)
            except RateLimitedError as e:
                # Backoff inside the client is already exhausted. Stop the sweep
                # rather than deepening the block; the weekly schedule retries.
                log.error(f"Aborting fundamentals sweep at {symbol}: {e}")
                aborted = True
                break

            if rows:
                total_rows += writer.insert_fundamentals(rows)
                with_data += 1
                consecutive_failures = 0
            else:
                without_data.append(symbol)
                consecutive_failures += 1
                if consecutive_failures >= YF_CIRCUIT_BREAKER:
                    log.error(
                        f"Circuit breaker: {consecutive_failures} consecutive symbols "
                        f"returned nothing (last: {symbol}). Yahoo is likely blocking us."
                    )
                    aborted = True
                    break

            if i % 50 == 0:
                log.info(f"  {i}/{len(symbols)} symbols — {total_rows:,} rows so far")

            client.pause_between_symbols()

    log.info(
        f"Fundamentals: {with_data}/{len(symbols)} symbols had data, "
        f"{total_rows:,} rows written"
        + (" (ABORTED EARLY)" if aborted else "")
    )
    # ~24% of IDX symbols genuinely have no fundamentals on Yahoo — measured, not a fault.
    if without_data:
        log.info(f"  no fundamentals: {len(without_data)} symbols "
                 f"(sample: {without_data[:10]})")

    return {
        'symbols_total': len(symbols),
        'symbols_with_data': with_data,
        'symbols_without_data': len(without_data),
        'rows_written': total_rows,
        'aborted': aborted,
    }


# ---------------------------------------------------------------------------
# Task — valuation + analyst snapshot
# ---------------------------------------------------------------------------

@task(name="Collect valuation snapshot", retries=1, retry_delay_seconds=300)
def collect_valuation(symbols: List[str]) -> Dict:
    log = get_run_logger()
    client = YFinanceClient()

    # Snapshots are dated to midnight so DEDUP UPSERT KEYS(ts, symbol) makes a
    # re-run replace the day's row instead of appending a second one.
    ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    valuations: List[Dict] = []
    consecutive_failures = 0
    aborted = False

    for i, symbol in enumerate(symbols, 1):
        try:
            record = client.get_valuation(symbol)
        except RateLimitedError as e:
            log.error(f"Aborting valuation sweep at {symbol}: {e}")
            aborted = True
            break

        if record:
            valuations.append(record)
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= YF_CIRCUIT_BREAKER:
                log.error(f"Circuit breaker tripped at {symbol} — stopping.")
                aborted = True
                break

        if i % 100 == 0:
            log.info(f"  {i}/{len(symbols)} symbols")

        client.pause_between_symbols()

    with YFinanceWriter() as writer:
        rows = writer.insert_valuation(valuations, ts)

    log.info(f"Valuation: {rows} snapshots" + (" (ABORTED EARLY)" if aborted else ""))
    return {'symbols_total': len(symbols), 'valuation_rows': rows, 'aborted': aborted}


# ---------------------------------------------------------------------------
# Task — analyst estimates
# ---------------------------------------------------------------------------

@task(name="Collect analyst estimates", retries=1, retry_delay_seconds=300)
def collect_analyst(symbols: List[str]) -> Dict:
    """
    Separate from the valuation snapshot because it costs 3 extra calls per
    symbol while estimates only move on revisions — weekly capture is enough to
    track how they evolve, and daily would triple the daily rate-limit exposure.
    """
    log = get_run_logger()
    client = YFinanceClient()
    ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    analysts: List[Dict] = []
    aborted = False

    for i, symbol in enumerate(symbols, 1):
        try:
            analysts.extend(client.get_analyst(symbol))
        except RateLimitedError as e:
            log.error(f"Aborting analyst sweep at {symbol}: {e}")
            aborted = True
            break
        if i % 100 == 0:
            log.info(f"  {i}/{len(symbols)} symbols")
        client.pause_between_symbols()

    with YFinanceWriter() as writer:
        rows = writer.insert_analyst(analysts, ts)

    log.info(f"Analyst: {rows} rows" + (" (ABORTED EARLY)" if aborted else ""))
    return {'analyst_rows': rows, 'aborted': aborted}


# ---------------------------------------------------------------------------
# Task — profile refresh
# ---------------------------------------------------------------------------

@task(name="Refresh profiles", retries=1, retry_delay_seconds=300)
def collect_profiles(symbols: List[str]) -> Dict:
    log = get_run_logger()
    client = YFinanceClient()
    ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    profiles: List[Dict] = []
    for i, symbol in enumerate(symbols, 1):
        try:
            record = client.get_profile(symbol)
        except RateLimitedError as e:
            log.error(f"Aborting profile refresh at {symbol}: {e}")
            break
        if record:
            profiles.append(record)
        if i % 100 == 0:
            log.info(f"  {i}/{len(symbols)} symbols")
        client.pause_between_symbols()

    with YFinanceWriter() as writer:
        rows = writer.insert_profile(profiles, ts)

    log.info(f"Profiles: {rows} rows")
    return {'profile_rows': rows}


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name="yfinance Fundamentals", log_prints=True)
def fundamentals_flow(
    stocks_file: str = "config/syariah_stocks.txt",
    limit: Optional[int] = None,
    include_profiles: bool = True,
    include_analyst: bool = True,
) -> Dict:
    """Weekly sweep: financial statements, profiles and analyst estimates."""
    log = get_run_logger()
    symbols = load_symbols(stocks_file, limit)
    log.info(f"Fundamentals sweep over {len(symbols)} symbols")

    results = {'fundamentals': collect_fundamentals(symbols)}
    if include_profiles:
        results['profiles'] = collect_profiles(symbols)
    if include_analyst:
        results['analyst'] = collect_analyst(symbols)

    log.info(f"Done: {results}")
    return results


@flow(name="yfinance Valuation Snapshot", log_prints=True)
def valuation_flow(
    stocks_file: str = "config/syariah_stocks.txt",
    limit: Optional[int] = None,
) -> Dict:
    """Daily snapshot: valuation metrics only (1 request per symbol)."""
    log = get_run_logger()
    symbols = load_symbols(stocks_file, limit)
    log.info(f"Valuation snapshot over {len(symbols)} symbols")

    result = collect_valuation(symbols)
    log.info(f"Done: {result}")
    return result


if __name__ == "__main__":
    fundamentals_flow(limit=5)
