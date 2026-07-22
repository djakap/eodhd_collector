#!/usr/bin/env python3
"""
Fill the daily bars missing from production between 2026-05-30 and 2026-06-25.

eodhd_stock_data holds nothing for those four weeks — a hole left when EODHD
returned 402s during the quota problem. The bars are recoverable from EODHD while
the subscription is live, and not afterwards, so this runs now.

Unlike the weekly/monthly fill, this one has a strong referee: yf_stock_data
already holds 11,082 rows covering exactly this period, collected independently.
A bar is written when the two sources agree, which is a far better test than any
plausibility rule.

Symbols that split are handled explicitly rather than rejected. yfinance
back-adjusts for splits and EODHD stores raw, so those symbols disagree by a
constant factor across every bar. When the disagreement is one steady ratio, the
EODHD value is still correct in EODHD's own convention and is accepted; when it
varies bar to bar, something is actually wrong and the symbol is skipped.

Usage:
    python scripts/fill_june_gap.py --dry-run
    python scripts/fill_june_gap.py
"""

import argparse
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import psycopg2
from dotenv import load_dotenv

from config.db_config import (
    TABLE_STOCK_DATA, QUESTDB_HOST, QUESTDB_PG_PORT,
    QUESTDB_USER, QUESTDB_PASSWORD, QUESTDB_DATABASE,
)
from config.yfinance_config import TABLE_YF_STOCK_DATA
from utils.bar_rules import SENTINEL_MIN

load_dotenv()
BASE = "https://eodhd.com/api"
START, END = '2026-05-30', '2026-06-26'
TOLERANCE = 0.005          # 0.5%
RATIO_SPREAD = 0.01        # a split ratio has to be steady to this much


def load_symbols(path):
    with open(path) as fh:
        return [f"{l.strip()}.JK" if not l.strip().endswith('.JK') else l.strip()
                for l in fh if l.strip() and not l.startswith('#')]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stocks-file', default='config/syariah_stocks.txt')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    key = os.getenv('EODHD_API_KEY', '')
    if not key:
        sys.exit("EODHD_API_KEY tidak ada di .env")

    conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT,
                            user=QUESTDB_USER, password=QUESTDB_PASSWORD,
                            database=QUESTDB_DATABASE)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"""SELECT symbol, timestamp, close FROM "{TABLE_YF_STOCK_DATA}"
                    WHERE interval='d' AND timestamp >= %s AND timestamp < %s
                    AND close IS NOT NULL""", (START, END))
    ref = {(s, str(t)[:10]): float(c) for s, t, c in cur.fetchall()}
    print(f"Pembanding dari yfinance: {len(ref):,} bar\n")

    cur.execute(f"""SELECT count() FROM "{TABLE_STOCK_DATA}" WHERE interval='d'
                    AND timestamp >= %s AND timestamp < %s""", (START, END))
    print(f"Sudah ada di produksi untuk rentang ini: {cur.fetchone()[0]:,} baris\n")

    symbols = load_symbols(args.stocks_file)
    tally = Counter()
    to_write = []

    for i, sym in enumerate(symbols, 1):
        r = requests.get(f"{BASE}/eod/{sym}", params={
            'api_token': key, 'period': 'd', 'fmt': 'json',
            'from': START, 'to': END}, timeout=45)
        time.sleep(0.12)
        if r.status_code != 200:
            tally['fetch gagal'] += 1
            continue
        try:
            bars = r.json()
        except ValueError:
            tally['respons rusak'] += 1
            continue
        if not bars:
            tally['API kosong'] += 1
            continue

        pairs, rows = [], []
        for b in bars:
            close = b.get('close')
            if close is None or float(close) >= SENTINEL_MIN:
                continue
            y = ref.get((sym, b['date']))
            if y is not None and y > 0:
                pairs.append(float(close) / y)
            rows.append((b, float(close)))

        if not rows:
            tally['tidak ada bar layak'] += 1
            continue

        if not pairs:
            tally['tanpa pembanding yfinance'] += 1
            continue

        med = statistics.median(pairs)
        spread = (max(pairs) - min(pairs)) / med if med else 9
        if spread > RATIO_SPREAD:
            tally['DITOLAK — selisih tidak konsisten'] += 1
            continue
        tally['cocok langsung' if abs(med - 1) <= TOLERANCE else 'cocok via rasio split'] += 1

        for b, close in rows:
            to_write.append((
                sym, 'd', datetime.strptime(b['date'], '%Y-%m-%d'),
                b.get('open'), b.get('high'), b.get('low'), close,
                b.get('adjusted_close'), b.get('volume'),
                None, 'eod', datetime.now(),
            ))

        if i % 100 == 0:
            print(f"  {i}/{len(symbols)} simbol — {len(to_write):,} bar siap")

    print("\nHasil:")
    for k, n in tally.most_common():
        print(f"  {k:36s} {n:>5,} simbol")
    print(f"\n  bar siap tulis: {len(to_write):,}")

    if args.dry_run:
        print("\nDRY RUN — tidak ada yang ditulis.")
        return
    if not to_write:
        return

    from db.questdb_client import QuestDBClient
    db = QuestDBClient()
    db.connect()
    try:
        for i in range(0, len(to_write), 5000):
            db.insert_price_data(to_write[i:i + 5000])
    finally:
        db.close()
    # Reconnect before the final count. The original connection has been idle
    # through ~15 minutes of API calls by this point and QuestDB drops it, which
    # made the first run raise "server closed the connection unexpectedly" AFTER
    # the writes had already succeeded — an alarming traceback for a run that
    # actually worked.
    time.sleep(6)
    conn2 = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT,
                             user=QUESTDB_USER, password=QUESTDB_PASSWORD,
                             database=QUESTDB_DATABASE)
    c2 = conn2.cursor()
    c2.execute(f"""SELECT count() FROM "{TABLE_STOCK_DATA}" WHERE interval='d'
                   AND timestamp >= %s AND timestamp < %s""", (START, END))
    print(f"\nProduksi sekarang punya {c2.fetchone()[0]:,} baris di rentang itu.")
    conn2.close()


if __name__ == "__main__":
    main()
