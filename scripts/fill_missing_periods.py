#!/usr/bin/env python3
"""
Fill the weekly/monthly bars EODHD serves but we do not hold.

The reconciliation found 4,471 of them (w 3,582, m 889). They are real bars we
simply never collected, and once the subscription lapses there is no way to get
them, so this runs while it is still live.

Every fetched bar is VALIDATED before it is written. That is not caution for its
own sake: the same reconciliation showed EODHD's weekly and monthly endpoints
returning implausible values on scattered periods — SPMA came back as 1941.44 for
a week whose daily bars ranged 236 to 246 — and 424 stored rows were found to be
more trustworthy than the API's current answer. Writing whatever the API says
would import that damage.

The referee is the daily data we already hold: a weekly or monthly bar has to sit
inside the daily high/low range of the period it covers. Bars that fail, or that
have no daily data to check against, are reported and skipped rather than written.

Usage:
    python scripts/fill_missing_periods.py --dry-run
    python scripts/fill_missing_periods.py
"""

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import psycopg2
from dotenv import load_dotenv

from config.db_config import (
    TABLE_STOCK_DATA, QUESTDB_HOST, QUESTDB_PG_PORT,
    QUESTDB_USER, QUESTDB_PASSWORD, QUESTDB_DATABASE,
)
from utils.bar_rules import SENTINEL_MIN

load_dotenv()
BASE = "https://eodhd.com/api"
REQUEST_DELAY = 0.15


def connect():
    return psycopg2.connect(
        host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
    )


def period_of(interval, d: datetime):
    """The period a bar covers, independent of the day it is labelled with."""
    if interval == 'm':
        return (d.year, d.month)
    iso = d.isocalendar()
    return (iso[0], iso[1])


def filled_periods(cur, interval):
    """
    Periods that already hold a bar, keyed by (symbol, period).

    Asking "is this DATE present?" is not enough, and getting it wrong cost 651
    duplicate June bars on the first run of this script. EODHD restamps a monthly
    bar once the month is final: ITMA's June 2026 bar sat at 2026-06-24 and the
    API now returns it as 2026-06-01, with the same close of 1325. Same bar, two
    labels. A date-based check sees the second as missing and adds it beside the
    first.
    """
    cur.execute(f"""SELECT symbol, timestamp FROM "{TABLE_STOCK_DATA}"
                    WHERE interval = %s""", (interval,))
    return {(s, period_of(interval, t)) for s, t in cur.fetchall()}


def daily_range(cur, symbol, start, days):
    """High/low of the daily bars covering this period — the referee."""
    end = start + timedelta(days=days)
    cur.execute(f"""SELECT min(low), max(high) FROM "{TABLE_STOCK_DATA}"
                    WHERE symbol=%s AND interval='d' AND timestamp >= %s
                    AND timestamp < %s AND close IS NOT NULL
                    AND close < {SENTINEL_MIN}""",
                (symbol, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
    r = cur.fetchone()
    return (float(r[0]), float(r[1])) if r and r[0] is not None else None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest', default='reports/period_reconciliation.json')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    key = os.getenv('EODHD_API_KEY', '')
    if not key:
        sys.exit("EODHD_API_KEY tidak ada di .env")

    missing = defaultdict(set)
    for row in json.load(open(args.manifest)).get('api_only', []):
        missing[(row['symbol'], row['interval'])].add(row['date'])
    print(f"Bar hilang menurut manifest: "
          f"{sum(len(v) for v in missing.values()):,} "
          f"pada {len(missing):,} pasangan simbol/interval\n")

    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()

    occupied = {iv: filled_periods(cur, iv) for iv in ('w', 'm')}
    print("Periode yang sudah terisi (apa pun tanggalnya): " +
          ", ".join(f"{k}={len(v):,}" for k, v in occupied.items()) + "\n")

    tally = Counter()
    to_write = []
    for i, ((sym, iv), dates) in enumerate(sorted(missing.items()), 1):
        r = requests.get(f"{BASE}/eod/{sym}", params={
            'api_token': key, 'period': iv, 'fmt': 'json'}, timeout=45)
        time.sleep(REQUEST_DELAY)
        if r.status_code != 200:
            tally['fetch gagal'] += len(dates)
            continue
        try:
            api = {x['date']: x for x in r.json()}
        except ValueError:
            tally['respons rusak'] += len(dates)
            continue

        for d in dates:
            bar = api.get(d)
            if bar is None:
                tally['tidak ada lagi di API'] += 1
                continue
            close = bar.get('close')
            if close is None or float(close) >= SENTINEL_MIN:
                tally['sentinel / kosong'] += 1
                continue

            start = datetime.strptime(d, '%Y-%m-%d')
            if (sym, period_of(iv, start)) in occupied[iv]:
                tally['periode sudah terisi (tanggal beda)'] += 1
                continue
            rng = daily_range(cur, sym, start, 31 if iv == 'm' else 7)
            if rng is None:
                tally['tanpa data harian pembanding'] += 1
                continue
            lo, hi = rng
            pad = (hi - lo) * 0.02 + 0.5
            if not (lo - pad <= float(close) <= hi + pad):
                tally['DITOLAK — di luar rentang harian'] += 1
                continue

            tally['lolos'] += 1
            to_write.append((
                sym, iv, start,
                bar.get('open'), bar.get('high'), bar.get('low'), close,
                bar.get('adjusted_close'), bar.get('volume'),
                None, 'eod', datetime.now(),
            ))

        if i % 100 == 0:
            print(f"  {i}/{len(missing)} pasangan — lolos {tally['lolos']:,}")

    print("\nHasil validasi:")
    for k, n in tally.most_common():
        print(f"  {k:34s} {n:>7,}")

    if args.dry_run:
        print(f"\nDRY RUN — {len(to_write):,} baris SIAP tulis, tidak ada yang ditulis.")
        return

    if not to_write:
        print("\nTidak ada yang bisa ditulis.")
        return

    from db.questdb_client import QuestDBClient
    db = QuestDBClient()
    db.connect()
    try:
        for i in range(0, len(to_write), 5000):
            db.insert_price_data(to_write[i:i + 5000])
    finally:
        db.close()
    time.sleep(5)
    print(f"\n{len(to_write):,} baris ditulis ke {TABLE_STOCK_DATA}.")


if __name__ == "__main__":
    main()
