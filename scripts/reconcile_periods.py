#!/usr/bin/env python3
"""
Reconcile stored weekly/monthly bars against EODHD's own response.

WRITES NOTHING TO THE DATABASE. It produces a report and a JSON manifest of the
rows it believes are surplus, so the deletion can be reviewed before it happens.

Why the API and not a rule: every local rule tried so far was wrong. Testing
date_trunc('month') flagged 84,672 valid rows, because EODHD stamps a monthly bar
on the first TRADING day, not the first calendar day (BRIS.JK 2025-04-08 is real,
and stored values match the API exactly). The API is the only authority for which
row is genuine.

What surplus looks like — BRIS.JK, Jan-Feb 2026:

    API returns  2 monthly bars   (2026-01-01, 2026-02-02)
    DB holds    14                (+2026-01-07, -14, -21, -25, -26, -28, ...)

The extras are leftovers from incremental fetches: each run wrote a partial
month-to-date aggregate under that run's date. DEDUP could not absorb them
because the timestamp itself differed, which is the whole mechanism behind the
1.39x row inflation on the 'm' interval.

TIMING: this needs a live EODHD subscription. Once it lapses there is no way left
to decide which rows are genuine, so it must run before cutover.

Usage:
    python scripts/reconcile_periods.py --limit 5        # try it small first
    python scripts/reconcile_periods.py                  # all 651 symbols
"""

import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import psycopg2
from dotenv import load_dotenv

from config.db_config import (
    TABLE_STOCK_DATA, QUESTDB_HOST, QUESTDB_PG_PORT,
    QUESTDB_USER, QUESTDB_PASSWORD, QUESTDB_DATABASE,
)

load_dotenv()

BASE = "https://eodhd.com/api"
PERIODS = ['w', 'm']

# EODHD returns prices rounded differently across endpoints; compare with a
# tolerance rather than exact equality so rounding is not reported as a conflict.
PRICE_EPS = 0.01

REQUEST_DELAY = 0.15      # ~1,300 calls; polite pacing, well inside quota


def load_symbols(path, limit=None):
    with open(path) as fh:
        syms = [f"{l.strip()}.JK" if not l.strip().endswith('.JK') else l.strip()
                for l in fh if l.strip() and not l.startswith('#')]
    return syms[:limit] if limit else syms


class Reconciler:
    def __init__(self, api_key, table):
        self.key = api_key
        self.table = table
        self.conn = psycopg2.connect(
            host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
            password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
        )
        self.cur = self.conn.cursor()
        self.stats = Counter()
        self.surplus = []       # rows the API does not know about
        self.conflicts = []     # same date, different values
        self.api_only = []      # API has it, we do not
        self.failed = []

    def fetch(self, symbol, period):
        r = requests.get(f"{BASE}/eod/{symbol}", params={
            'api_token': self.key, 'period': period, 'fmt': 'json',
        }, timeout=45)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        try:
            return r.json(), None
        except ValueError:
            return None, "respons bukan JSON"

    @staticmethod
    def _current_period_start(period):
        """First date of the period still in progress, as 'YYYY-MM-DD'."""
        today = datetime.now()
        if period == 'm':
            return today.replace(day=1).strftime('%Y-%m-%d')
        monday = today.toordinal() - today.weekday()
        return datetime.fromordinal(monday).strftime('%Y-%m-%d')

    def stored(self, symbol, period):
        self.cur.execute(
            f'SELECT timestamp, open, high, low, close FROM "{self.table}" '
            f'WHERE symbol = %s AND interval = %s ORDER BY timestamp',
            (symbol, period))
        return {str(r[0])[:10]: r for r in self.cur.fetchall()}

    def reconcile(self, symbol, period):
        api, err = self.fetch(symbol, period)
        if err:
            self.failed.append((symbol, period, err))
            self.stats['gagal'] += 1
            return
        api_by_date = {row['date']: row for row in api}
        db = self.stored(symbol, period)

        self.stats[f'{period}_api'] += len(api_by_date)
        self.stats[f'{period}_db'] += len(db)

        for date, row in db.items():
            if date not in api_by_date:
                self.surplus.append({
                    'symbol': symbol, 'interval': period, 'date': date,
                    'close': float(row[4]) if row[4] is not None else None,
                })
                self.stats[f'{period}_surplus'] += 1
                continue
            a = api_by_date[date]
            if row[4] is not None and a.get('close') is not None:
                if abs(float(row[4]) - float(a['close'])) > PRICE_EPS:
                    # The period still running is refreshed by EODHD after our last
                    # sweep, so a difference there measures collection timing, not
                    # a defect. Counted apart so it cannot inflate the real signal.
                    in_progress = date >= self._current_period_start(period)
                    self.conflicts.append({
                        'symbol': symbol, 'interval': period, 'date': date,
                        'db_close': float(row[4]), 'api_close': float(a['close']),
                        'in_progress': in_progress,
                    })
                    self.stats[f'{period}_konflik_berjalan' if in_progress
                               else f'{period}_konflik'] += 1
                    continue
            self.stats[f'{period}_cocok'] += 1

        for date in api_by_date:
            if date not in db:
                self.api_only.append({'symbol': symbol, 'interval': period, 'date': date})
                self.stats[f'{period}_hanya_api'] += 1

    def run(self, symbols):
        total = len(symbols) * len(PERIODS)
        done = 0
        for sym in symbols:
            for period in PERIODS:
                self.reconcile(sym, period)
                done += 1
                time.sleep(REQUEST_DELAY)
            if len(self.failed) >= 20:
                print(f"\nBERHENTI: {len(self.failed)} panggilan gagal berturut — "
                      f"periksa kuota/langganan.")
                break
            if done % 100 == 0:
                print(f"  {done}/{total} panggilan — surplus sejauh ini: "
                      f"{len(self.surplus):,}")
        self.conn.close()

    def report(self, out_path):
        print(f"\n{'=' * 74}\nHASIL REKONSILIASI\n{'=' * 74}")
        for period in PERIODS:
            label = {'w': 'MINGGUAN', 'm': 'BULANAN'}[period]
            print(f"\n{label}")
            print(f"  bar menurut API      : {self.stats[f'{period}_api']:>8,}")
            print(f"  baris tersimpan      : {self.stats[f'{period}_db']:>8,}")
            print(f"  cocok                : {self.stats[f'{period}_cocok']:>8,}")
            print(f"  SURPLUS (tak dikenal): {self.stats[f'{period}_surplus']:>8,}")
            print(f"  konflik nilai        : {self.stats[f'{period}_konflik']:>8,}")
            print(f"  konflik periode jalan: {self.stats[f'{period}_konflik_berjalan']:>8,}"
                  f"  (wajar — API lebih segar)")
            print(f"  hanya ada di API     : {self.stats[f'{period}_hanya_api']:>8,}")

        real = [c for c in self.conflicts if not c['in_progress']]
        if real:
            print(f"\n  PERHATIAN — {len(real):,} baris bertanggal sama tapi nilainya")
            print(f"  beda, di periode yang SUDAH SELESAI. Ini bukan surplus dan tidak")
            print(f"  masuk daftar hapus; perlu diperiksa terpisah. Contoh:")
            for c in real[:5]:
                print(f"    {c['symbol']:10s} {c['interval']} {c['date']}  "
                      f"db={c['db_close']} api={c['api_close']}")

        if self.failed:
            print(f"\n  {len(self.failed)} panggilan gagal (contoh: {self.failed[:3]})")

        payload = {
            'generated': datetime.now().isoformat(timespec='seconds'),
            'table': self.table,
            'stats': dict(self.stats),
            'surplus': self.surplus,
            'conflicts': self.conflicts,
            'api_only': self.api_only,
        }
        with open(out_path, 'w') as fh:
            json.dump(payload, fh, indent=2)
        print(f"\n  manifest ditulis: {out_path}")
        print(f"  berisi {len(self.surplus):,} baris surplus — TIDAK ADA YANG DIHAPUS.")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stocks-file', default='config/syariah_stocks.txt')
    p.add_argument('--limit', type=int)
    p.add_argument('--table', default=TABLE_STOCK_DATA)
    p.add_argument('--out', default='reports/period_reconciliation.json')
    args = p.parse_args()

    key = os.getenv('EODHD_API_KEY', '')
    if not key:
        sys.exit("EODHD_API_KEY tidak ada di .env")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    symbols = load_symbols(args.stocks_file, args.limit)
    print(f"Rekonsiliasi {len(symbols)} simbol x {len(PERIODS)} periode "
          f"= {len(symbols) * len(PERIODS)} panggilan API")
    print("Skrip ini tidak menulis apa pun ke database.\n")

    r = Reconciler(key, args.table)
    r.run(symbols)
    r.report(args.out)


if __name__ == "__main__":
    main()
