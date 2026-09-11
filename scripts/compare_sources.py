#!/usr/bin/env python3
"""
Compare EODHD against yfinance, bar for bar.

This is the instrument the migration decision rests on. It answers three questions
the parallel run exists to settle:

  1. Do the two sources agree on price and volume where both have a bar?
  2. Does either source have bars the other is missing?
  3. Does yfinance keep up across all 651 symbols, day after day?

Four classes of difference are NOT defects, and are separated out so they do not
drown the real signal. Each was confirmed against the data on 2026-07-21:

  * out-of-session rows — eodhd_stock_data carries 141,193 rows (0.51%) stamped
    outside IDX hours, shifted duplicates left by collectors run from the WIB host
    before db/questdb_client pinned ILP timestamps to UTC. Counting them shows as
    phantom "EODHD-only" bars. Pass --include-out-of-session to see them.

  * all-NULL bars — EODHD emits a 16:00 WIB (09:00 UTC) 1h bar with every OHLCV
    field NULL. All 459 found in one week were empty; yfinance omits them. These
    are absence of data, not disagreement, so they are counted separately.

  * in-progress periods — today's bar, this week's weekly bar and this month's
    monthly bar are still moving. EODHD's sweep runs at 19:30 WIB, so comparing
    before then measures collection timing, not data. Excluded by default;
    --include-partial keeps them.

  * split-adjusted history — yfinance back-adjusts OHLC and volume for splits,
    EODHD stores raw. A symbol mid-split shows a clean integer price ratio
    (MLPT 25.0, RAJA 5.0, RMKE 5.0). Reported as a separate SPLIT class rather
    than as a price mismatch.

  * symbols outside the watchlist — the bulk-eod deployment fetches the WHOLE JK
    exchange, so eodhd_stock_data holds 867 daily symbols against the watchlist's
    651. The extra 216 have ~3 rows each and are not collected by yfinance at all,
    which showed as 642 phantom "EODHD-only" daily bars. The comparison is scoped
    to the watchlist; --all-symbols removes the scoping.

Note on QuestDB 7.3.10: hour(timestamp) is safe as a WHERE predicate but silently
drops the range filter when grouped on, returning whole-table counts. Do not add
GROUP BY hour(timestamp) here.

Usage:
    python scripts/compare_sources.py --days 7
    python scripts/compare_sources.py --interval 1h --symbols BRIS.JK,ANTM.JK
"""

import argparse
import logging
import sys
import os
from collections import Counter
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT,
    QUESTDB_USER, QUESTDB_PASSWORD, QUESTDB_DATABASE,
)
from config.tables import TABLE_PRICES_LEGACY_EODHD, TABLE_PRICES_PRODUCTION

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

# IDX trades 09:00-16:00 WIB, stored as UTC.
SESSION_START_UTC, SESSION_END_UTC = 2, 9

# IDX ticks in whole rupiah, so a 1-rupiah move on a Rp 50 stock is 2%. A
# tolerance tighter than the tick itself would flag arithmetic, not disagreement.
PRICE_TOLERANCE_PCT = 0.5
VOLUME_TOLERANCE_PCT = 1.0   # last-tick timing legitimately moves volume slightly

# --days is expressed in calendar days, which starves the coarse intervals: a
# 7-day window leaves one weekly bar, and the in-progress one at that. Each
# interval is widened to whatever holds a useful number of completed periods.
MIN_WINDOW_DAYS = {'w': 90, 'm': 730}

# A split shows up as a near-exact integer ratio between the two closes.
SPLIT_RATIOS = (2, 2.5, 4, 5, 8, 10, 20, 25, 40, 50, 100)
SPLIT_RATIO_EPS = 0.01


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


def _partial_cutoff(interval: str):
    """
    Start of the period still in progress. Bars at or after this are excluded
    unless --include-partial, because the two sources are simply refreshed at
    different times of day.
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if interval == 'w':
        return today - timedelta(days=today.weekday())
    if interval == 'm':
        return today.replace(day=1)
    return today          # d, 1h and friends: everything before today


def _period_key(interval: str, ts):
    """
    The identity of a bar, for matching across sources.

    For intraday and daily the timestamp IS the identity. For weekly and monthly
    it is not, because the two sources anchor the period differently: EODHD stamps
    a monthly bar on the first TRADING day and yfinance on the first CALENDAR day,
    so AADI's December 2024 bar is 2024-12-05 in one and 2024-12-01 in the other.
    Matching on the raw timestamp made every single monthly bar appear as both
    "only EODHD" and "only yfinance" — 100% false mismatch. Weekly has the same
    problem whenever a Monday is a holiday.

    So w/m are keyed by the period they cover, not the day they are labelled with.
    """
    if interval == 'm':
        return (ts.year, ts.month)
    if interval == 'w':
        iso = ts.isocalendar()
        return (iso[0], iso[1])
    return ts


def fetch(cur, table: str, interval: str, since: str, symbols,
          include_all: bool, cutoff):
    sql = (f"SELECT symbol, timestamp, open, high, low, close, volume "
           f'FROM "{table}" WHERE interval = %s AND timestamp >= %s')
    params = [interval, since]
    if cutoff is not None:
        sql += " AND timestamp < %s"
        params.append(cutoff.strftime('%Y-%m-%d %H:%M:%S'))
    if symbols:
        placeholders = ', '.join(['%s'] * len(symbols))
        sql += f" AND symbol IN ({placeholders})"
        params.extend(symbols)
    sql += _session_filter(interval, include_all)

    cur.execute(sql, params)
    return {(r[0], _period_key(interval, r[1])): r for r in cur.fetchall()}


def _is_empty(row) -> bool:
    """Every OHLCV field NULL — a placeholder bar, not a quote."""
    return all(row[i] is None for i in (2, 3, 4, 5, 6))


def _looks_like_split(a, b) -> bool:
    if not a or not b:
        return False
    ratio = max(float(a), float(b)) / min(float(a), float(b))
    return any(abs(ratio - r) < SPLIT_RATIO_EPS for r in SPLIT_RATIOS)


def compare(interval: str, days: int, symbols, include_all: bool,
            include_partial: bool) -> dict:
    window = max(days, MIN_WINDOW_DAYS.get(interval, 0))
    since = (datetime.now() - timedelta(days=window)).strftime('%Y-%m-%d')
    cutoff = None if include_partial else _partial_cutoff(interval)

    conn = connect()
    cur = conn.cursor()
    eodhd = fetch(cur, TABLE_PRICES_LEGACY_EODHD, interval, since, symbols, include_all, cutoff)
    yfin = fetch(cur, TABLE_PRICES_PRODUCTION, interval, since, symbols, include_all, cutoff)
    conn.close()

    # Placeholder rows are absence of data; comparing them as "EODHD-only bars"
    # would report a defect where neither source disagrees with the other.
    empty_eodhd = {k for k, v in eodhd.items() if _is_empty(v)}
    empty_yf = {k for k, v in yfin.items() if _is_empty(v)}
    eodhd = {k: v for k, v in eodhd.items() if k not in empty_eodhd}
    yfin = {k: v for k, v in yfin.items() if k not in empty_yf}

    both = set(eodhd) & set(yfin)
    only_eodhd = set(eodhd) - set(yfin)
    only_yf = set(yfin) - set(eodhd)

    price_diffs, volume_diffs, split_symbols = [], [], Counter()
    for key in both:
        e, y = eodhd[key], yfin[key]
        if e[5] and y[5]:
            if _looks_like_split(e[5], y[5]):
                split_symbols[key[0]] += 1
                continue
            price_diffs.append((abs(float(e[5]) - float(y[5])) / float(e[5]) * 100, key))
        if e[6] and y[6]:
            volume_diffs.append((abs(float(e[6]) - float(y[6])) / float(e[6]) * 100, key))

    return {
        'interval': interval,
        'since': since,
        'cutoff': cutoff,
        'eodhd_bars': len(eodhd),
        'yf_bars': len(yfin),
        'matched': len(both),
        'only_eodhd': only_eodhd,
        'only_yf': only_yf,
        'empty_eodhd': len(empty_eodhd),
        'empty_yf': len(empty_yf),
        'price_diffs': price_diffs,
        'volume_diffs': volume_diffs,
        'split_symbols': split_symbols,
        'bad_price': [d for d in price_diffs if d[0] > PRICE_TOLERANCE_PCT],
        'bad_volume': [d for d in volume_diffs if d[0] > VOLUME_TOLERANCE_PCT],
    }


def report(r: dict) -> bool:
    print(f"\n{'=' * 76}")
    cut = f"  s/d {r['cutoff']:%Y-%m-%d}" if r['cutoff'] else "  (termasuk periode berjalan)"
    print(f"interval '{r['interval']}'  sejak {r['since']}{cut}")
    print('=' * 76)
    print(f"  bar EODHD          : {r['eodhd_bars']:>9,}")
    print(f"  bar yfinance       : {r['yf_bars']:>9,}")
    print(f"  timestamp cocok    : {r['matched']:>9,}")
    print(f"  hanya di EODHD     : {len(r['only_eodhd']):>9,}")
    print(f"  hanya di yfinance  : {len(r['only_yf']):>9,}")
    if r['empty_eodhd'] or r['empty_yf']:
        print(f"  bar kosong diabaikan: EODHD {r['empty_eodhd']:,}, yfinance {r['empty_yf']:,}")

    if r['price_diffs']:
        exact = sum(1 for d, _ in r['price_diffs'] if d == 0)
        worst = max(r['price_diffs'])
        print(f"\n  close identik      : {exact:>9,} / {len(r['price_diffs']):,}"
              f"  ({exact / len(r['price_diffs']) * 100:.2f}%)")
        print(f"  beda close maks    : {worst[0]:.4f}%  ({worst[1][0]} {worst[1][1]})")
        print(f"  di atas toleransi  : {len(r['bad_price']):,} (> {PRICE_TOLERANCE_PCT}%)")
    if r['volume_diffs']:
        worst = max(r['volume_diffs'])
        print(f"  beda volume maks   : {worst[0]:.2f}%  ({worst[1][0]} {worst[1][1]})")
        print(f"  di atas toleransi  : {len(r['bad_volume']):,} dari {len(r['volume_diffs']):,}")

    if r['split_symbols']:
        print(f"\n  SPLIT — yfinance menyesuaikan mundur, EODHD menyimpan mentah:")
        for sym, n in r['split_symbols'].most_common(10):
            print(f"    {sym:10s} {n:,} bar")

    for label, keys in (('hanya EODHD', r['only_eodhd']), ('hanya yfinance', r['only_yf'])):
        if keys:
            print(f"\n  contoh {label}:")
            for sym, ts in sorted(keys)[:5]:
                print(f"    {sym:10s} {ts}")

    clean = not r['bad_price'] and not r['only_eodhd']
    print(f"\n  VERDIKT: {'SEPADAN' if clean else 'ADA SELISIH — periksa di atas'}")
    return clean


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--interval', default='all',
                        help="d, w, m, 1h, or 'all' (default)")
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--symbols', help="comma-separated, default the watchlist")
    parser.add_argument('--stocks-file', default='config/syariah_stocks.txt',
                        help="watchlist the comparison is scoped to")
    parser.add_argument('--all-symbols', action='store_true',
                        help="compare every symbol present, including the whole-exchange "
                             "rows bulk-eod pulls in beyond the watchlist")
    parser.add_argument('--include-out-of-session', action='store_true',
                        help="do not filter the known shifted rows")
    parser.add_argument('--include-partial', action='store_true',
                        help="also compare today / the in-progress week and month")
    args = parser.parse_args()

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.all_symbols:
        symbols = None
    else:
        from flows.fundamentals_flow import load_symbols
        symbols = load_symbols(args.stocks_file, None)
        print(f"dibandingkan pada {len(symbols)} simbol watchlist "
              f"({args.stocks_file}); pakai --all-symbols untuk seluruh bursa")

    intervals = ['d', 'w', 'm', '1h'] if args.interval == 'all' else [args.interval]

    all_clean = True
    for interval in intervals:
        result = compare(interval, args.days, symbols,
                         args.include_out_of_session, args.include_partial)
        if result['eodhd_bars'] == 0 and result['yf_bars'] == 0:
            print(f"\ninterval '{interval}': tidak ada data di rentang ini")
            continue
        all_clean &= report(result)

    print()
    sys.exit(0 if all_clean else 1)


if __name__ == "__main__":
    main()
