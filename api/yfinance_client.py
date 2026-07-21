"""
yfinance API Client

Fetches fundamentals, valuation and analyst data for IDX (.JK) symbols.

yfinance is an unofficial scraper of Yahoo Finance, not a contracted API. It has
no quota to exceed but it DOES get rate-limited by IP and request pattern
(YFRateLimitError), and a fundamentals sweep issues several requests per symbol.
Everything here is built around not tripping that:

  - fixed delay + jitter between calls (lockstep patterns look like bots)
  - exponential backoff on rate-limit errors specifically
  - a circuit breaker, because once Yahoo starts refusing, continuing to hammer
    deepens the block rather than recovering from it
"""

import random
import re
import time
import logging
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError

from config.yfinance_config import (
    YF_REQUEST_DELAY,
    YF_JITTER,
    YF_SYMBOL_DELAY,
    YF_MAX_RETRIES,
    YF_BACKOFF_BASE,
    YF_BACKOFF_MAX,
    YF_PERCENT_FIELDS,
    VALUATION_FIELDS,
    PROFILE_FIELDS,
    STATEMENTS,
)

logger = logging.getLogger(__name__)


class RateLimitedError(RuntimeError):
    """Raised when a symbol could not be fetched because Yahoo is rate-limiting."""


# Our interval codes -> Yahoo's. The rest of the codebase speaks the EODHD codes
# already stored in eodhd_stock_data.interval, so the mapping lives here only.
INTERVAL_MAP = {
    'd': '1d',
    'w': '1wk',
    'm': '1mo',
    '1h': '1h',
    '5m': '5m',
    '15m': '15m',
    '30m': '30m',
}

# Bars whose timestamp carries a time of day, and therefore needs UTC conversion.
INTRADAY_CODES = frozenset({'1h', '5m', '15m', '30m'})

# Yahoo's own history limits, measured 2026-07-20 against .JK symbols. Requesting
# beyond these returns an empty frame rather than an error.
MAX_LOOKBACK_DAYS = {
    '5m': 60,
    '15m': 60,
    '30m': 60,
    '1h': 730,
}


# Matching is word-boundary, never substring: 'Operations' contains "ratio" and
# 'Administration' contains "ratio", so a naive `'ratio' in name` misclassifies
# 'Net Income Continuous Operations' as a ratio and silently drops it out of any
# absolute-value aggregation.
_RATIO_EXACT = {'tax rate for calcs', 'tax rate'}
_RATIO_WORDS = re.compile(r'\b(ratio|margin|yield|percent)\b')
_PER_SHARE_WORDS = re.compile(r'\b(eps|per share)\b')
_COUNT_WORDS = re.compile(r'\bshares?\b')


def classify_line_item(name: str) -> str:
    """
    Classify a financial-statement line item by unit.

    A single statement mixes absolute currency amounts, ratios, per-share values
    and share counts. Storing them without this marker invites summing 'Net
    Income' together with 'Tax Rate For Calcs'.

    'rate' alone is deliberately NOT a ratio trigger: 'Effect Of Exchange Rate
    Changes' is an absolute cash-flow amount. Genuine rate items are listed
    explicitly instead.
    """
    n = name.lower()
    if _PER_SHARE_WORDS.search(n):
        return 'per_share'
    if n in _RATIO_EXACT or _RATIO_WORDS.search(n):
        return 'ratio'
    if _COUNT_WORDS.search(n):
        return 'count'
    return 'absolute'


class YFinanceClient:
    """Client for yfinance with throttling and rate-limit backoff."""

    def __init__(self, request_delay: float = YF_REQUEST_DELAY,
                 jitter: float = YF_JITTER):
        self.request_delay = request_delay
        self.jitter = jitter
        self.last_request_time = 0.0

    # -- pacing -------------------------------------------------------------

    def _throttle(self):
        """Space out requests, with jitter so the pattern is not perfectly regular."""
        elapsed = time.time() - self.last_request_time
        wait = self.request_delay - elapsed
        if wait > 0:
            time.sleep(wait)
        if self.jitter:
            time.sleep(random.uniform(0, self.jitter))
        self.last_request_time = time.time()

    def _fetch(self, label: str, fn):
        """
        Run a yfinance accessor with throttling and backoff.

        Rate-limit errors get exponential backoff and are re-raised as
        RateLimitedError when exhausted, so the caller can trip its breaker.
        Other errors return None — a symbol simply lacking fundamentals is
        normal on IDX (measured ~24% of tracked symbols) and must not abort a sweep.
        """
        for attempt in range(YF_MAX_RETRIES):
            try:
                self._throttle()
                return fn()
            except YFRateLimitError:
                backoff = min(YF_BACKOFF_BASE * (2 ** attempt), YF_BACKOFF_MAX)
                logger.warning(
                    f"Rate limited on {label} (attempt {attempt + 1}/{YF_MAX_RETRIES}), "
                    f"backing off {backoff:.0f}s"
                )
                time.sleep(backoff)
            except Exception as e:
                logger.debug(f"{label} unavailable: {type(e).__name__}: {e}")
                return None

        raise RateLimitedError(f"Rate limited on {label} after {YF_MAX_RETRIES} attempts")

    def pause_between_symbols(self):
        if YF_SYMBOL_DELAY:
            time.sleep(YF_SYMBOL_DELAY + random.uniform(0, self.jitter))

    # -- fetchers -----------------------------------------------------------

    def get_fundamentals(self, symbol: str, currency: Optional[str] = None) -> List[Dict]:
        """
        Fetch all six financial statements, flattened to long/narrow rows.

        Returns a list of dicts ready for yf_fundamentals. Empty list if the
        symbol has no fundamentals at all.

        When `currency` is not supplied it is resolved from info['financialCurrency'],
        which is the REPORTING currency. Several IDX issuers (Adaro-group coal
        names among them) report in USD while trading in IDR, so absolute values
        are not comparable across issuers without it.
        """
        ticker = yf.Ticker(symbol)
        rows: List[Dict] = []

        if currency is None:
            info = self._fetch(f"{symbol}.info", lambda: ticker.info)
            currency = (info or {}).get('financialCurrency')

        for attr, statement, freq in STATEMENTS:
            df = self._fetch(f"{symbol}.{attr}", lambda a=attr: getattr(ticker, a))
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                continue

            for period in df.columns:
                period_ts = pd.Timestamp(period).to_pydatetime()
                for line_item in df.index:
                    value = df.at[line_item, period]
                    if pd.isna(value):
                        continue
                    rows.append({
                        'symbol': symbol,
                        'period_end': period_ts,
                        'statement': statement,
                        'freq': freq,
                        'line_item': str(line_item),
                        'value': float(value),
                        'value_kind': classify_line_item(str(line_item)),
                        'currency': currency,
                    })

        return rows

    def get_valuation(self, symbol: str) -> Optional[Dict]:
        """
        Fetch Ticker.info and map it to yf_valuation_daily columns.

        Ratio units are normalised to fractions here — see YF_PERCENT_FIELDS.
        """
        info = self._fetch(f"{symbol}.info", lambda: yf.Ticker(symbol).info)
        if not info:
            return None

        record: Dict = {
            'symbol': symbol,
            'currency': info.get('currency'),
            # Kept alongside `currency` so downstream can detect the mismatch that
            # silently corrupts Yahoo's derived ratios — see schemas_yfinance.sql.
            'financial_currency': info.get('financialCurrency'),
        }
        for src, col in VALUATION_FIELDS.items():
            value = info.get(src)
            if value is None:
                record[col] = None
                continue
            try:
                value = float(value)
            except (TypeError, ValueError):
                record[col] = None
                continue
            if src in YF_PERCENT_FIELDS:
                value = value / 100.0
            record[col] = value

        # A row with no market cap is not a usable snapshot.
        return record if record.get('market_cap') is not None else None

    def get_analyst(self, symbol: str) -> List[Dict]:
        """Fetch price targets and per-period EPS/revenue estimates."""
        ticker = yf.Ticker(symbol)
        rows: List[Dict] = []

        targets = self._fetch(f"{symbol}.analyst_price_targets",
                              lambda: ticker.analyst_price_targets)
        if targets:
            rows.append({
                'symbol': symbol,
                'period': 'target',
                'target_current': _num(targets.get('current')),
                'target_high': _num(targets.get('high')),
                'target_low': _num(targets.get('low')),
                'target_mean': _num(targets.get('mean')),
                'target_median': _num(targets.get('median')),
            })

        eps = self._fetch(f"{symbol}.earnings_estimate", lambda: ticker.earnings_estimate)
        rev = self._fetch(f"{symbol}.revenue_estimate", lambda: ticker.revenue_estimate)

        periods = set()
        if isinstance(eps, pd.DataFrame) and not eps.empty:
            periods |= set(eps.index)
        if isinstance(rev, pd.DataFrame) and not rev.empty:
            periods |= set(rev.index)

        for period in periods:
            row = {'symbol': symbol, 'period': str(period)}
            if isinstance(eps, pd.DataFrame) and period in eps.index:
                row['eps_avg'] = _num(eps.at[period, 'avg']) if 'avg' in eps.columns else None
                row['eps_low'] = _num(eps.at[period, 'low']) if 'low' in eps.columns else None
                row['eps_high'] = _num(eps.at[period, 'high']) if 'high' in eps.columns else None
                row['num_analysts'] = (_num(eps.at[period, 'numberOfAnalysts'])
                                       if 'numberOfAnalysts' in eps.columns else None)
                row['growth'] = _num(eps.at[period, 'growth']) if 'growth' in eps.columns else None
                row['currency'] = eps.at[period, 'currency'] if 'currency' in eps.columns else None
            if isinstance(rev, pd.DataFrame) and period in rev.index:
                row['revenue_avg'] = _num(rev.at[period, 'avg']) if 'avg' in rev.columns else None
                row['revenue_low'] = _num(rev.at[period, 'low']) if 'low' in rev.columns else None
                row['revenue_high'] = _num(rev.at[period, 'high']) if 'high' in rev.columns else None

            # Skip periods where every estimate is missing (common for '0q'/'+1q' on IDX).
            if any(row.get(k) is not None for k in
                   ('eps_avg', 'eps_low', 'eps_high', 'revenue_avg', 'num_analysts')):
                rows.append(row)

        return rows

    # -- price / corporate actions ------------------------------------------

    def get_price_history(self, symbol: str, interval: str,
                          start=None, end=None, period: Optional[str] = None) -> List[Dict]:
        """
        Fetch OHLCV bars for one interval, normalised to the storage convention.

        `interval` is OUR code ('d', 'w', 'm', '1h', '5m', ...), not Yahoo's — the
        caller should never have to know the mapping.

        auto_adjust=False so Close stays the raw close and Adj Close is available
        separately, matching how EODHD populates close vs adjusted_close. With
        auto_adjust=True (the yfinance default) Close is silently the adjusted
        series and would not line up with the existing rows.
        """
        yf_interval = INTERVAL_MAP.get(interval)
        if yf_interval is None:
            raise ValueError(f"Unsupported interval: {interval}")

        kwargs = dict(interval=yf_interval, auto_adjust=False)
        if period:
            kwargs['period'] = period
        else:
            if start:
                kwargs['start'] = start
            if end:
                kwargs['end'] = end

        df = self._fetch(f"{symbol}.history({interval})",
                         lambda: yf.Ticker(symbol).history(**kwargs))
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return []

        intraday = interval in INTRADAY_CODES
        records: List[Dict] = []
        for ts, row in df.iterrows():
            close = _num(row.get('Close'))
            if close is None:
                continue
            open_ = _num(row.get('Open'))
            high = _num(row.get('High'))
            low = _num(row.get('Low'))
            if None in (open_, high, low):
                continue
            volume = _num(row.get('Volume'))
            records.append({
                'symbol': symbol,
                'interval': interval,
                'timestamp': _to_storage_timestamp(ts, intraday),
                'open': open_,
                'high': high,
                'low': low,
                'close': close,
                'adjusted_close': _num(row.get('Adj Close')) if not intraday else None,
                'volume': int(volume) if volume is not None else None,
                'source': 'intraday' if intraday else 'eod',
            })
        return records

    def get_dividends(self, symbol: str) -> List[Dict]:
        """Full dividend history, shaped for insert_corporate_actions()."""
        series = self._fetch(f"{symbol}.dividends", lambda: yf.Ticker(symbol).dividends)
        if series is None or not len(series):
            return []
        return [{
            'symbol': symbol,
            'action_type': 'dividend',
            'action_date': _to_storage_timestamp(ts, intraday=False),
            'dividend_amount': _num(value),
            'dividend_currency': 'IDR' if symbol.endswith('.JK') else None,
        } for ts, value in series.items()]

    def get_splits(self, symbol: str) -> List[Dict]:
        """
        Full split history, shaped for insert_corporate_actions().

        yfinance reports the ratio as a single float (5.0 for a 5-for-1), whereas
        the table stores from/to plus the original string. A value of 0 means "no
        split" and is dropped.
        """
        series = self._fetch(f"{symbol}.splits", lambda: yf.Ticker(symbol).splits)
        if series is None or not len(series):
            return []

        records = []
        for ts, value in series.items():
            ratio = _num(value)
            if not ratio:
                continue
            records.append({
                'symbol': symbol,
                'action_type': 'split',
                'action_date': _to_storage_timestamp(ts, intraday=False),
                'split_ratio': f"{ratio:g}/1",
                'split_from': int(ratio) if float(ratio).is_integer() else None,
                'split_to': 1,
            })
        return records

    def get_profile(self, symbol: str) -> Optional[Dict]:
        """Fetch descriptive company data (sector, industry, ISIN, ...)."""
        info = self._fetch(f"{symbol}.info", lambda: yf.Ticker(symbol).info)
        if not info:
            return None

        record: Dict = {'symbol': symbol}
        for src, col in PROFILE_FIELDS.items():
            record[col] = info.get(src)

        isin = self._fetch(f"{symbol}.isin", lambda: yf.Ticker(symbol).isin)
        # yfinance returns '-' rather than None when it has no ISIN.
        record['isin'] = isin if isin and isin != '-' else None

        employees = record.get('full_time_employees')
        try:
            record['full_time_employees'] = int(employees) if employees is not None else None
        except (TypeError, ValueError):
            record['full_time_employees'] = None

        return record


def _to_storage_timestamp(ts, intraday: bool):
    """
    Convert a yfinance bar index to the convention already in eodhd_stock_data.

    This is the single most dangerous conversion in the migration. yfinance returns
    tz-AWARE timestamps in the exchange timezone (WIB for .JK), while the table holds
    27.7M rows written by the EODHD collector as tz-NAIVE values:

      intraday  ->  naive UTC   (IDX 09:00 WIB is stored as 02:00)
      EOD/w/m   ->  naive date at midnight

    Dropping the tz without converting would place every intraday bar 7 hours off,
    silently, and it would still look plausible. Verified against BRIS.JK 2026-07-15,
    where EODHD's first 1h bar is stored at 02:00.
    """
    ts = pd.Timestamp(ts)
    if intraday:
        if ts.tz is not None:
            ts = ts.tz_convert('UTC').tz_localize(None)
        return ts.to_pydatetime()
    # Daily and coarser: the calendar date is what matters, not the session time.
    if ts.tz is not None:
        ts = ts.tz_localize(None)
    return ts.normalize().to_pydatetime()


def _num(value) -> Optional[float]:
    """Coerce to float, mapping NaN/None/non-numeric to None."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(out) else out
