#!/usr/bin/env python3
"""
Derive 4h bars from stored 1h bars in stock_data.

yfinance serves 4h natively, but it is an EXACT aggregation of 1h (verified 24/24
bars identical, OHLCV to the unit), so deriving from the 1h we already collect
costs no API calls, reaches the full 1h history (2024-08+, vs yfinance's 730-day
4h window), and is provably the same series.

Bucketing matches yfinance's native 4h, which is why they agree: IDX trades
09:00-16:00 WIB = 02:00-09:00 UTC, split into a morning bar (UTC 02:00-04:00,
stamped 02:00 = 09:00 WIB) and an afternoon bar (UTC 06:00-09:00, stamped 06:00 =
13:00 WIB). The 05:00 UTC lunch hour is excluded.

DEDUP on (timestamp, symbol, interval) makes re-derivation idempotent, so this can
run incrementally after every intraday 1h fetch (pass --since) or as a full
rebuild (no --since).

Usage:
    python scripts/derive_4h.py                 # full history
    python scripts/derive_4h.py --since 2026-07-01
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)

TABLE = 'stock_data'

# Session-hour → 4h bucket. Morning bar covers UTC hours 2,3,4; afternoon 6,7,8,9.
BUCKET = ("CASE WHEN hour(timestamp) IN (2,3,4) "
          "THEN dateadd('h',2,date_trunc('day',timestamp)) "
          "WHEN hour(timestamp) IN (6,7,8,9) "
          "THEN dateadd('h',6,date_trunc('day',timestamp)) END")


def derive(since: str = None) -> int:
    conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                            password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)
    conn.autocommit = True
    cur = conn.cursor()

    where = ("interval='1h' AND hour(timestamp) IN (2,3,4,6,7,8,9) "
             "AND open IS NOT NULL")
    if since:
        where += f" AND timestamp >= '{since}'"

    cur.execute(f"""
        INSERT INTO {TABLE}
        SELECT symbol, '4h' as interval, {BUCKET} as ts,
            first(open), max(high), min(low), last(close), last(adjusted_close),
            sum(volume), 0, 'derived_4h', now()
        FROM {TABLE}
        WHERE {where}
        GROUP BY symbol, {BUCKET}
    """)
    time.sleep(6)      # let the WAL apply before counting
    scope = f" (>= {since})" if since else ""
    cur.execute(f"SELECT count() FROM {TABLE} WHERE interval='4h'"
                + (f" AND timestamp >= '{since}'" if since else ""))
    n = cur.fetchone()[0]
    conn.close()
    print(f"4h bars in scope{scope}: {n:,}")
    return n


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--since', help="only derive from 1h at or after this date")
    derive(p.parse_args().since)
