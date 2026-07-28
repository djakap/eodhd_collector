"""
Prefect Flow: live intraday collection during the IDX session.

Fetches 1h bars for all watchlist symbols in ONE batched yfinance call (yf.download),
writes them to stock_data, then derives 4h from the updated 1h. Runs hourly during
the session so 1h refreshes every hour and 4h stays current (it completes at the
12:00 and 16:00 WIB session boundaries).

Why batch, not the per-symbol collector: yf.download pulls all 651 symbols in ~19
seconds in a single call, versus 651 sequential calls (~11 minutes) the per-symbol
path would make. That keeps hourly intraday collection light enough to run every
hour without tripping yfinance's rate limit — the whole reason "all 651, hourly"
is feasible.

The current hour's bar is partial and updates on each fetch (DEDUP replaces on the
business key); the 21:30 end-of-day run finalises it. Downstream analysis that
needs only settled bars should exclude the current, still-forming period.
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import warnings
warnings.filterwarnings('ignore')

from prefect import flow, task, get_run_logger

from db.questdb_client import QuestDBClient
from flows.fundamentals_flow import load_symbols
from scripts.derive_4h import derive as derive_4h

PROD = 'stock_data'
LOOKBACK_DAYS = 3          # short window; DEDUP absorbs the overlap, revisions caught


@task(name="Batch fetch 1h", retries=1, retry_delay_seconds=120)
def batch_fetch_1h(symbols: List[str], days: int) -> List:
    """All symbols' 1h bars in one yf.download call → insert tuples."""
    import yfinance as yf

    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    df = yf.download(symbols, interval='1h', start=start, auto_adjust=False,
                     group_by='ticker', progress=False, threads=True)

    now = datetime.now()
    rows = []
    for sym in symbols:
        if sym not in df.columns.get_level_values(0):
            continue
        sub = df[sym].dropna(subset=['Close'])
        for ts, r in sub.iterrows():
            # yf.download timestamps are tz-aware; store naive UTC per the table's
            # convention (the same to_utc_naive rule the per-symbol path uses).
            utc = ts.tz_convert('UTC').tz_localize(None) if ts.tzinfo else ts
            rows.append((
                sym, '1h', utc.to_pydatetime(),
                float(r['Open']), float(r['High']), float(r['Low']), float(r['Close']),
                None, int(r['Volume']) if r['Volume'] == r['Volume'] else None,
                None, 'intraday_live', now,
            ))
    return rows


@task(name="Write 1h + derive 4h")
def write_and_derive(rows: List) -> Dict:
    log = get_run_logger()
    if not rows:
        return {'written': 0, 'symbols': 0}

    db = QuestDBClient()
    db.connect()
    try:
        for i in range(0, len(rows), 5000):
            db.insert_price_data(rows[i:i + 5000], table=PROD)
    finally:
        db.close()
    time.sleep(4)

    # derive 4h only for the days we just touched
    since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    derive_4h(since=since)

    symbols = len({r[0] for r in rows})
    log.info(f"1h ditulis: {len(rows):,} bar untuk {symbols} simbol; 4h di-derive sejak {since}")
    return {'written': len(rows), 'symbols': symbols}


@flow(name="Intraday Live", log_prints=True)
def intraday_flow(stocks_file: str = "config/syariah_stocks.txt",
                  days: int = LOOKBACK_DAYS) -> Dict:
    log = get_run_logger()
    symbols = load_symbols(stocks_file, None)
    log.info(f"Batch-fetch 1h untuk {len(symbols)} simbol (lookback {days}h)")
    rows = batch_fetch_1h(symbols, days)
    result = write_and_derive(rows)
    log.info(f"Intraday selesai: {result}")
    return result


if __name__ == "__main__":
    intraday_flow()
