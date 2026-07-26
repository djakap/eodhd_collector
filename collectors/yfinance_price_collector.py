"""
yfinance price collector — replacement for price_collector / bulk_collector /
action_collector.

Writes into the SAME eodhd_stock_data and eodhd_corporate_actions tables as the
EODHD collectors, deliberately: both tables now carry DEDUP on their business key,
so the two sources can run side by side during the migration and the overlap
collapses instead of doubling. That is what makes a parallel-run comparison
possible at all.

Two things this collector has to get right, both verified against the 27.7M rows
already stored (see _to_storage_timestamp in api/yfinance_client):

  * timezone — yfinance returns tz-aware WIB, the table holds naive UTC for
    intraday and naive midnight dates for EOD.
  * adjusted close — Yahoo's auto_adjust default folds adjustments into Close;
    we keep raw Close and Adj Close apart, as EODHD does.

There is no bulk endpoint here. EODHD's bulk API fetched a whole exchange in one
request; yfinance is per-symbol, so the daily sweep is 651 calls per interval.
Measured safe: the daily valuation snapshot completed 650 symbols with zero rate
limit errors.
"""

import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.yfinance_client import YFinanceClient, RateLimitedError, MAX_LOOKBACK_DAYS
from db.questdb_client import QuestDBClient
from config.db_config import TABLE_STOCK_DATA
from config.yfinance_config import (
    YF_EOD_PERIODS,
    YF_INTRADAY_INTERVALS,
    YF_EOD_PERIOD_FULL,
    YF_INTRADAY_FULL_DAYS,
    YF_UPDATE_WINDOW_DAYS,
    YF_PRICE_TABLE,
    TABLE_YF_STOCK_DATA,
    TABLE_CORPORATE_ACTIONS_YF,
)

logger = logging.getLogger(__name__)


class YFinancePriceCollector:
    """Collects OHLCV and corporate actions from yfinance into QuestDB."""

    def __init__(self, update_mode: bool = False,
                 update_window: int = YF_UPDATE_WINDOW_DAYS,
                 target_table: str = YF_PRICE_TABLE,
                 actions_table: str = TABLE_CORPORATE_ACTIONS_YF):
        self.update_mode = update_mode
        self.update_window = update_window
        self.target_table = target_table
        self.actions_table = actions_table
        self.api = YFinanceClient()
        self.db = QuestDBClient()
        self.db.connect()

    def close(self):
        try:
            self.db.close()
        finally:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- helpers ------------------------------------------------------------

    def _window_for(self, symbol: str, interval: str):
        """
        Resolve the fetch window for one symbol/interval.

        Incremental runs start slightly before the last stored bar so corrections
        are picked up; DEDUP absorbs the re-sent rows. Full runs reach as far back
        as Yahoo allows, which for 5m/15m/30m is only 60 days.
        """
        if self.update_mode:
            last = self._last_bar(symbol, interval)
            if last:
                start = last - timedelta(days=self.update_window)
                return start.strftime('%Y-%m-%d'), None, None

        if interval in YF_EOD_PERIODS:
            return None, None, YF_EOD_PERIOD_FULL

        days = min(YF_INTRADAY_FULL_DAYS, MAX_LOOKBACK_DAYS.get(interval, 60))
        start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        return start, None, None

    def _last_bar(self, symbol: str, interval: str) -> Optional[datetime]:
        """
        Latest bar already stored for this symbol/interval, read from the target
        table itself rather than eodhd_stock_metadata — during the parallel run
        that metadata tracks EODHD's progress, not ours.
        """
        try:
            self.db.ensure_connection()
            self.db.cursor.execute(
                f'SELECT max(timestamp) FROM "{self.target_table}" '
                f'WHERE symbol = %s AND interval = %s',
                (symbol, interval)
            )
            row = self.db.cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.debug(f"No prior bar for {symbol}/{interval}: {e}")
            return None

    def _store(self, symbol: str, interval: str, records: List[Dict]) -> int:
        if not records:
            return 0

        now = datetime.now()
        rows = [(
            r['symbol'], r['interval'], r['timestamp'],
            r['open'], r['high'], r['low'], r['close'],
            r['adjusted_close'], r['volume'],
            None,                      # gmtoffset — yfinance gives none
            r['source'], now,
        ) for r in records]

        self.db.insert_price_data(rows, table=self.target_table)

        # Metadata tracking is only meaningful once this collector owns the
        # production table; while shadowing it would overwrite EODHD's freshness
        # markers and confuse gap-check.
        if self.target_table == TABLE_STOCK_DATA:
            stamps = [r['timestamp'] for r in records]
            self.db.upsert_stock_metadata(
                symbol, interval,
                data_start=min(stamps), data_end=max(stamps),
                total_records=len(rows),
            )
        return len(rows)

    # -- collection ---------------------------------------------------------

    def collect_eod(self, symbol: str, periods: Optional[List[str]] = None) -> int:
        total = 0
        for period in (periods or YF_EOD_PERIODS):
            start, end, span = self._window_for(symbol, period)
            records = self.api.get_price_history(symbol, period,
                                                 start=start, end=end, period=span)
            written = self._store(symbol, period, records)
            if written:
                logger.info(f"{symbol} {period}: {written} bars")
            total += written
        return total

    def collect_intraday(self, symbol: str, intervals: Optional[List[str]] = None) -> int:
        total = 0
        for interval in (intervals or YF_INTRADAY_INTERVALS):
            start, end, span = self._window_for(symbol, interval)
            records = self.api.get_price_history(symbol, interval,
                                                 start=start, end=end, period=span)
            written = self._store(symbol, interval, records)
            if written:
                logger.info(f"{symbol} {interval}: {written} bars")
            total += written
        return total

    def collect_actions(self, symbol: str) -> int:
        """
        Dividends and splits.

        Always fetched in full — yfinance returns the entire history per call and
        offers no date filter. DEDUP on (action_date, symbol, action_type) makes
        the repetition free, which is exactly why that table was rebuilt first.

        Writes to the fresh corporate_actions table (self.actions_table), kept
        apart from the legacy eodhd_corporate_actions so yfinance's adjusted
        dividend amounts do not mix with EODHD's. DEDUP on the business key makes
        the full re-fetch idempotent. Skipped while the price collector is still
        shadowing a non-production table.
        """
        records = self.api.get_dividends(symbol) + self.api.get_splits(symbol)
        if not records:
            return 0
        self.db.insert_corporate_actions(records, table=self.actions_table)
        return len(records)

    @property
    def is_shadowing(self) -> bool:
        # Production is stock_data now (TABLE_YF_STOCK_DATA). Writing anywhere else
        # — a scratch or comparison table — is shadowing, and shadowing skips
        # corporate actions. Before cutover this compared against eodhd_stock_data;
        # that table is legacy, so the check now names the live production table.
        return self.target_table != TABLE_YF_STOCK_DATA

    def collect_all(self, symbol: str, skip_intraday: bool = False,
                    skip_actions: Optional[bool] = None) -> Dict:
        if skip_actions is None:
            skip_actions = self.is_shadowing

        result = {'symbol': symbol, 'eod': 0, 'intraday': 0, 'actions': 0, 'error': None}
        try:
            result['eod'] = self.collect_eod(symbol)
            if not skip_intraday:
                result['intraday'] = self.collect_intraday(symbol)
            if not skip_actions:
                result['actions'] = self.collect_actions(symbol)
        except RateLimitedError:
            raise
        except Exception as e:
            logger.error(f"{symbol}: {type(e).__name__}: {e}")
            result['error'] = str(e)
        finally:
            self.api.pause_between_symbols()
        return result
