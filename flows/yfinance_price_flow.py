"""
Prefect Flow: yfinance price collection (parallel-run phase)

Writes OHLCV into yf_stock_data — a shadow of eodhd_stock_data — so both sources
can be compared bar for bar before EODHD is retired. Writing them into one table
would not work: it dedups on (timestamp, symbol, interval), so whichever source
ran last would be the only one left.

Two entry points:

  backfill_flow  — one-off, fetches everything yfinance will give (decades of
                   d/w/m, ~730 days of 1h). Run once to seed the shadow table.

  update_flow    — daily incremental, re-fetching a short window so late
                   corrections are picked up. This is what runs alongside the
                   EODHD deployments during the parallel period.

Corporate actions are deliberately not collected here: there is no shadow table
for them, so writing would overwrite EODHD's dividend values. The collector skips
them automatically while shadowing.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from typing import Dict, List, Optional

from prefect import flow, task, get_run_logger

from api.yfinance_client import RateLimitedError
from collectors.yfinance_price_collector import YFinancePriceCollector
from config.yfinance_config import YF_CIRCUIT_BREAKER, YF_PRICE_TABLE
from flows.fundamentals_flow import load_symbols

logger = logging.getLogger(__name__)


@task(name="Collect yfinance prices", retries=1, retry_delay_seconds=300)
def collect_prices(symbols: List[str], update_mode: bool,
                   update_window: int, skip_intraday: bool) -> Dict:
    log = get_run_logger()

    totals = {'eod': 0, 'intraday': 0}
    failures: List[str] = []
    consecutive_failures = 0
    aborted = False

    with YFinancePriceCollector(update_mode=update_mode,
                                update_window=update_window) as collector:
        log.info(f"Writing to {collector.target_table} "
                 f"(shadow={collector.is_shadowing}, update_mode={update_mode})")

        for i, symbol in enumerate(symbols, 1):
            try:
                result = collector.collect_all(symbol, skip_intraday=skip_intraday)
            except RateLimitedError as e:
                log.error(f"Aborting at {symbol}: {e}")
                aborted = True
                break

            if result['error'] or (result['eod'] == 0 and result['intraday'] == 0):
                failures.append(symbol)
                consecutive_failures += 1
                if consecutive_failures >= YF_CIRCUIT_BREAKER:
                    log.error(f"Circuit breaker: {consecutive_failures} consecutive "
                              f"symbols returned nothing (last: {symbol}).")
                    aborted = True
                    break
            else:
                consecutive_failures = 0
                totals['eod'] += result['eod']
                totals['intraday'] += result['intraday']

            if i % 50 == 0:
                log.info(f"  {i}/{len(symbols)} — {totals['eod']:,} EOD, "
                         f"{totals['intraday']:,} intraday bars")

    log.info(f"Done: {totals['eod']:,} EOD + {totals['intraday']:,} intraday bars, "
             f"{len(failures)} symbols with no data"
             + (" (ABORTED EARLY)" if aborted else ""))
    if failures:
        log.info(f"  no data: {len(failures)} (sample: {failures[:10]})")

    return {
        'symbols_total': len(symbols),
        'symbols_failed': len(failures),
        'eod_bars': totals['eod'],
        'intraday_bars': totals['intraday'],
        'aborted': aborted,
    }


@flow(name="yfinance Price Backfill", log_prints=True)
def backfill_flow(stocks_file: str = "config/syariah_stocks.txt",
                  limit: Optional[int] = None,
                  skip_intraday: bool = False) -> Dict:
    """One-off seed of the shadow table with all history yfinance offers."""
    log = get_run_logger()
    symbols = load_symbols(stocks_file, limit)
    log.info(f"Backfilling {len(symbols)} symbols into {YF_PRICE_TABLE}")
    result = collect_prices(symbols, update_mode=False,
                            update_window=0, skip_intraday=skip_intraday)
    log.info(f"Backfill result: {result}")
    return result


@flow(name="yfinance Price Update", log_prints=True)
def update_flow(stocks_file: str = "config/syariah_stocks.txt",
                limit: Optional[int] = None,
                update_window: int = 7,
                skip_intraday: bool = False) -> Dict:
    """Daily incremental run for the parallel period."""
    log = get_run_logger()
    symbols = load_symbols(stocks_file, limit)
    log.info(f"Updating {len(symbols)} symbols in {YF_PRICE_TABLE}")
    result = collect_prices(symbols, update_mode=True,
                            update_window=update_window, skip_intraday=skip_intraday)
    log.info(f"Update result: {result}")
    return result


if __name__ == "__main__":
    backfill_flow(limit=3)
