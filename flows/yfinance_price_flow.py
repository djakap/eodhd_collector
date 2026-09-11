"""
Prefect Flow: production yfinance price collection

Writes OHLCV into stock_data, the source-neutral production price table.
eodhd_stock_data is the frozen legacy EODHD table retained for reconciliation
and historical coverage.

Two entry points:

  backfill_flow  — one-off, fetches everything yfinance will give (decades of
                   d/w/m, ~730 days of 1h). Run once to seed the target table.

  update_flow    — daily incremental, re-fetching a short window so late
                   corrections are picked up. This is the active production
                   price update.

The separate actions_flow writes current yfinance actions to corporate_actions.
Price runs skip actions by default.
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
                   update_window: int, skip_intraday: bool,
                   skip_actions: bool = True) -> Dict:
    log = get_run_logger()

    totals = {'eod': 0, 'intraday': 0, 'actions': 0}
    failures: List[str] = []
    consecutive_failures = 0
    aborted = False

    with YFinancePriceCollector(update_mode=update_mode,
                                update_window=update_window) as collector:
        log.info(f"Writing to {collector.target_table} "
                 f"(shadow={collector.is_shadowing}, update_mode={update_mode}, "
                 f"skip_actions={skip_actions})")

        for i, symbol in enumerate(symbols, 1):
            try:
                result = collector.collect_all(symbol, skip_intraday=skip_intraday,
                                               skip_actions=skip_actions)
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
                totals['actions'] += result['actions']

            if i % 50 == 0:
                log.info(f"  {i}/{len(symbols)} — {totals['eod']:,} EOD, "
                         f"{totals['intraday']:,} intraday bars")

    log.info(f"Done: {totals['eod']:,} EOD + {totals['intraday']:,} intraday bars"
             f" + {totals['actions']:,} corporate actions, "
             f"{len(failures)} symbols with no data"
             + (" (ABORTED EARLY)" if aborted else ""))
    if failures:
        log.info(f"  no data: {len(failures)} (sample: {failures[:10]})")

    return {
        'symbols_total': len(symbols),
        'symbols_failed': len(failures),
        'eod_bars': totals['eod'],
        'intraday_bars': totals['intraday'],
        'action_records': totals['actions'],
        'aborted': aborted,
    }


@flow(name="yfinance Price Backfill", log_prints=True)
def backfill_flow(stocks_file: str = "config/syariah_stocks.txt",
                  limit: Optional[int] = None,
                  skip_intraday: bool = False) -> Dict:
    """One-off seed of the price table with all history yfinance offers, plus
    the full dividend/split history (skip_actions=False)."""
    log = get_run_logger()
    symbols = load_symbols(stocks_file, limit)
    log.info(f"Backfilling {len(symbols)} symbols into {YF_PRICE_TABLE}")
    result = collect_prices(symbols, update_mode=False, update_window=0,
                            skip_intraday=skip_intraday, skip_actions=False)
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
    # Nightly stays lean: prices only. Corporate actions rarely change and a full
    # re-fetch of all symbols would roughly double the yfinance call count and the
    # rate-limit exposure of every evening run. actions_flow refreshes them weekly.
    result = collect_prices(symbols, update_mode=True, update_window=update_window,
                            skip_intraday=skip_intraday, skip_actions=True)
    log.info(f"Update result: {result}")
    return result


@flow(name="yfinance Corporate Actions", log_prints=True)
def actions_flow(stocks_file: str = "config/syariah_stocks.txt",
                 limit: Optional[int] = None) -> Dict:
    """
    Collect dividends and splits only, into corporate_actions.

    Separate from the price flows and scheduled weekly, because actions change
    rarely and yfinance returns the whole history per call — nightly collection
    would be wasted API load. DEDUP on the business key makes each run idempotent.
    """
    log = get_run_logger()
    symbols = load_symbols(stocks_file, limit)
    log.info(f"Collecting corporate actions for {len(symbols)} symbols")

    total, failed, aborted = 0, 0, False
    with YFinancePriceCollector(update_mode=True) as collector:
        log.info(f"Writing actions to {collector.actions_table}")
        for i, symbol in enumerate(symbols, 1):
            try:
                total += collector.collect_actions(symbol)
            except RateLimitedError as e:
                log.error(f"Aborting at {symbol}: {e}")
                aborted = True
                break
            except Exception as e:
                log.error(f"{symbol}: {type(e).__name__}: {e}")
                failed += 1
            finally:
                collector.api.pause_between_symbols()
            if i % 100 == 0:
                log.info(f"  {i}/{len(symbols)} — {total:,} action records")

    log.info(f"Done: {total:,} action records, {failed} failures"
             + (" (ABORTED EARLY)" if aborted else ""))
    return {'symbols': len(symbols), 'action_records': total,
            'failed': failed, 'aborted': aborted}


if __name__ == "__main__":
    backfill_flow(limit=3)
