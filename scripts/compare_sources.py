#!/usr/bin/env python3
"""
Compare EODHD against yfinance, bar for bar.

This is the instrument the migration decision rests on. It answers three questions
the parallel run exists to settle:

  1. Do the two sources agree on price and volume where both have a bar?
  2. Does either source have bars the other is missing?
  3. Does yfinance keep up across all 651 symbols, day after day?

Out-of-session rows are excluded by default. eodhd_stock_data carries 141,193 rows
(0.51%) stamped outside IDX hours — shifted duplicates left by collectors that were
run from the WIB host before db/questdb_client pinned ILP timestamps to UTC. Counting
them would show as phantom "EODHD-only" bars and drown the real signal. Pass
--include-out-of-session to see them.

Usage:
    python scripts/compare_sources.py --days 7
    python scripts/compare_sources.py --interval 1h --symbols BRIS.JK,ANTM.JK
"""

import argparse
import logging
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config.db_config import (
    TABLE_STOCK_DATA, QUESTDB_HOST, QUESTDB_PG_PORT,
    QUESTDB_USER, QUESTDB_PASSWORD, QUESTDB_DATABASE,
)
from config.yfinance_config import TABLE_YF_STOCK_DATA

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

# IDX trades 09:00-16:00 WIB, stored as UTC.
SESSION_START_UTC, SESSION_END_UTC = 2, 9

PRICE_TOLERANCE_PCT = 0.01   # anything above this is a real disagreement
VOLUME_TOLERANCE_PCT = 1.0   # last-tick timing legitimately moves volume slightly


def connect():
    return psycopg2.connect(
        host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
    )


def _session_filter(interval: str, include_all: bool) -> str:
    if include_all:
        return ""
    if interval in ('d', 'w', 'm'):
        return " AND hour(timestamp) = 0"
    return (f" AND hour(timestamp) >= {SESSION_START_UTC}"
            f" AND hour(timestamp) <= {SESSION_END_UTC}")


def fetch(cur, table: str, interval: str, since: str, symbols, include_all: bool):
    sql = (f"SELECT symbol, timestamp, open, high, low, close, volume "
           f'FROM "{table}" WHERE interval = %s AND timestamp >= %s')
    params = [interval, since]
    if symbols:
        placeholders = ', '.join(['%s'] * len(symbols))
        sql += f" AND symbol IN ({placeholders})"
        params.extend(symbols)
    sql += _session_filter(interval, include_all)

    cur.execute(sql, params)
    return {(r[0], r[1]): r for r in cur.fetchall()}


def compare(interval: str, days: int, symbols, include_all: bool) -> dict:
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    conn = connect()
    cur = conn.cursor()

    eodhd = fetch(cur, TABLE_STOCK_DATA, interval, since, symbols, include_all)
    yfin = fetch(cur, TABLE_YF_STOCK_DATA, interval, since, symbols, include_all)
    conn.close()

    both = set(eodhd) & set(yfin)
    only_eodhd = set(eodhd) - set(yfin)
    only_yf = set(yfin) - set(eodhd)

    price_diffs = []
    volume_diffs = []
    for key in both:
        e, y = eodhd[key], yfin[key]
        if e[5] and y[5]:
            price_diffs.append((abs(float(e[5]) - float(y[5])) / float(e[5]) * 100, key))
        if e[6] and y[6]:
            volume_diffs.append((abs(float(e[6]) - float(y[6])) / float(e[6]) * 100, key))

    bad_price = [d for d in price_diffs if d[0] > PRICE_TOLERANCE_PCT]
    bad_volume = [d for d in volume_diffs if d[0] > VOLUME_TOLERANCE_PCT]

    return {
        'interval': interval,
        'since': since,
        'eodhd_bars': len(eodhd),
        'yf_bars': len(yfin),
        'matched': len(both),
        'only_eodhd': only_eodhd,
        'only_yf': only_yf,
        'price_diffs': price_diffs,
        'volume_diffs': volume_diffs,
        'bad_price': bad_price,
        'bad_volume': bad_volume,
    }


def report(r: dict) -> bool:
    print(f"\n{'=' * 76}")
    print(f"interval '{r['interval']}'  sejak {r['since']}")
    print('=' * 76)
    print(f"  bar EODHD          : {r['eodhd_bars']:>9,}")
    print(f"  bar yfinance       : {r['yf_bars']:>9,}")
    print(f"  timestamp cocok    : {r['matched']:>9,}")
    print(f"  hanya di EODHD     : {len(r['only_eodhd']):>9,}")
    print(f"  hanya di yfinance  : {len(r['only_yf']):>9,}")

    if r['price_diffs']:
        worst = max(r['price_diffs'])
        print(f"\n  beda close maks    : {worst[0]:.4f}%  ({worst[1][0]} {worst[1][1]})")
        print(f"  di atas toleransi  : {len(r['bad_price']):,} dari {len(r['price_diffs']):,}")
    if r['volume_diffs']:
        worst = max(r['volume_diffs'])
        print(f"  beda volume maks   : {worst[0]:.2f}%  ({worst[1][0]} {worst[1][1]})")
        print(f"  di atas toleransi  : {len(r['bad_volume']):,} dari {len(r['volume_diffs']):,}")

    for label, keys in (('hanya EODHD', r['only_eodhd']), ('hanya yfinance', r['only_yf'])):
        if keys:
            sample = sorted(keys)[:5]
            print(f"\n  contoh {label}:")
            for sym, ts in sample:
                print(f"    {sym:10s} {ts}")

    clean = not r['bad_price'] and not r['only_eodhd']
    print(f"\n  VERDIKT: {'SEPADAN' if clean else 'ADA SELISIH — periksa di atas'}")
    return clean


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--interval', default='all',
                        help="d, w, m, 1h, or 'all' (default)")
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--symbols', help="comma-separated, default all")
    parser.add_argument('--include-out-of-session', action='store_true',
                        help="do not filter the known shifted rows")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(',')] if args.symbols else None
    intervals = ['d', 'w', 'm', '1h'] if args.interval == 'all' else [args.interval]

    all_clean = True
    for interval in intervals:
        result = compare(interval, args.days, symbols, args.include_out_of_session)
        if result['eodhd_bars'] == 0 and result['yf_bars'] == 0:
            print(f"\ninterval '{interval}': tidak ada data di rentang ini")
            continue
        all_clean &= report(result)

    print()
    sys.exit(0 if all_clean else 1)


if __name__ == "__main__":
    main()
