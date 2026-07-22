#!/usr/bin/env python3
"""
How good is eodhd_stock_data right now?

Four questions, answered with measurements rather than assertions:

  1. COVERAGE     — how many symbols and trading days does each interval hold?
  2. CONSISTENCY  — do the intervals agree with each other? Aggregating 5m should
                    reproduce 1h, and 1h should reproduce the daily bar. This is
                    the strongest check available because it needs no external
                    source: the data is being tested against itself.
  3. ACCURACY     — does a fresh sample still match what EODHD serves?
  4. DEFECTS      — what is knowingly still wrong.

Read-only. Hits the API only for the accuracy sample.

Usage:  python scripts/quality_report.py [--no-api]
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from dotenv import load_dotenv

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)
from utils.bar_rules import SENTINEL_MIN

load_dotenv()
TABLE = 'eodhd_stock_data'
conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)
cur = conn.cursor()


def one(sql, params=None):
    cur.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None


def hr(t):
    print(f"\n{'=' * 76}\n{t}\n{'=' * 76}")


# ---------------------------------------------------------------- 1. coverage
hr("1. CAKUPAN")
print(f"{'iv':6s} {'baris':>12s} {'simbol':>7s} {'hari bursa':>11s} "
      f"{'rentang':>26s}")
for iv in ['5m', '15m', '1h', '4h', 'd', 'w', 'm']:
    n = one(f"SELECT count() FROM {TABLE} WHERE interval=%s", (iv,))
    if not n:
        continue
    syms = one(f"SELECT count_distinct(symbol) FROM {TABLE} WHERE interval=%s", (iv,))
    lo = one(f"SELECT min(timestamp) FROM {TABLE} WHERE interval=%s", (iv,))
    hi = one(f"SELECT max(timestamp) FROM {TABLE} WHERE interval=%s", (iv,))
    days = one(f"SELECT count() FROM (SELECT date_trunc('day', timestamp) d "
               f"FROM {TABLE} WHERE interval=%s GROUP BY d)", (iv,))
    print(f"{iv:6s} {n:>12,} {syms:>7,} {days:>11,} "
          f"{str(lo)[:10]} .. {str(hi)[:10]}")

# ------------------------------------------------------------ 2. consistency
hr("2. KONSISTENSI INTERNAL — apakah interval saling cocok?")
print("  Menjumlahkan 5m harus menghasilkan 1h; 1h harus menghasilkan bar harian.")
print("  Uji ini tidak butuh sumber luar — data diuji terhadap dirinya sendiri.\n")

SYMS = ['BRIS.JK', 'ANTM.JK', 'TLKM.JK', 'ASII.JK', 'ADRO.JK', 'UNTR.JK']
DAYS = ['2024-09-03', '2025-01-06', '2025-06-10', '2025-11-11',
        '2026-03-10', '2026-07-15']


def bars(sym, iv, day):
    cur.execute(
        f"SELECT timestamp, open, high, low, close, volume FROM {TABLE} "
        f"WHERE symbol=%s AND interval=%s AND timestamp >= %s AND timestamp < %s "
        f"AND close IS NOT NULL ORDER BY timestamp",
        (sym, iv, day, day + ' 23:59:59'))
    return cur.fetchall()


def roll_up(rows, minutes):
    """Aggregate fine bars into coarser buckets."""
    out = {}
    for ts, o, h, l, c, v in rows:
        key = ts.replace(minute=(ts.minute // minutes) * minutes if minutes < 60 else 0,
                         second=0, microsecond=0)
        b = out.setdefault(key, {'o': o, 'h': h, 'l': l, 'c': c, 'v': 0})
        if h is not None:
            b['h'] = h if b['h'] is None else max(b['h'], h)
        if l is not None:
            b['l'] = l if b['l'] is None else min(b['l'], l)
        b['c'] = c
        b['v'] += (v or 0)
    return out


def compare(derived, native, label, tally):
    for key, d in derived.items():
        n = native.get(key)
        if n is None:
            tally['tanpa padanan'] += 1
            continue
        same = all(
            (a is None) == (b is None) and (a is None or abs(float(a) - float(b)) < 0.01)
            for a, b in ((d['o'], n[1]), (d['h'], n[2]), (d['l'], n[3]), (d['c'], n[4])))
        tally['cocok' if same else 'BEDA'] += 1


t_5m_1h = defaultdict(int)
t_1h_d = defaultdict(int)
for day in DAYS:
    for sym in SYMS:
        b5, b1 = bars(sym, '5m', day), bars(sym, '1h', day)
        if b5 and b1:
            compare(roll_up(b5, 60), {r[0]: r for r in b1}, '5m->1h', t_5m_1h)
        bd = bars(sym, 'd', day)
        if b1 and bd:
            o = b1[0][1]
            h = max(r[2] for r in b1 if r[2] is not None)
            l = min(r[3] for r in b1 if r[3] is not None)
            c = b1[-1][4]
            n = bd[0]
            same = all(abs(float(a) - float(b)) < 0.01
                       for a, b in ((o, n[1]), (h, n[2]), (l, n[3]), (c, n[4])))
            t_1h_d['cocok' if same else 'BEDA'] += 1

print(f"  5m -> 1h : {dict(t_5m_1h)}")
print(f"  1h -> d  : {dict(t_1h_d)}")

# --------------------------------------------------------------- 3. accuracy
def accuracy():
    hr("3. AKURASI — sampel segar terhadap API EODHD")
    import requests
    key = os.getenv('EODHD_API_KEY', '')
    if not key:
        print("  EODHD_API_KEY tidak ada — dilewati")
        return
    ok = bad = 0
    for day in ['2024-09-03', '2025-06-10', '2026-03-10', '2026-07-15']:
        for sym in SYMS[:4]:
            for iv in ['5m', '1h']:
                s = int(datetime.strptime(day, '%Y-%m-%d')
                        .replace(tzinfo=timezone.utc).timestamp())
                r = requests.get(f"https://eodhd.com/api/intraday/{sym}", params={
                    'api_token': key, 'interval': iv, 'fmt': 'json',
                    'from': s, 'to': s + 86400}, timeout=45)
                if r.status_code != 200:
                    continue
                api = {d['datetime'][:16]: d for d in r.json()
                       if d.get('close') is not None}
                for ts, o, h, l, c, v in bars(sym, iv, day):
                    a = api.get(str(ts)[:16])
                    if a is None:
                        bad += 1
                    elif abs(float(c) - float(a['close'])) < 0.01:
                        ok += 1
                    else:
                        bad += 1
    total = ok + bad
    print(f"  {ok:,} dari {total:,} bar cocok persis "
          f"({ok / total * 100:.3f}%)" if total else "  tidak ada data")


# ---------------------------------------------------------------- 4. defects
def defects():
    hr("4. CACAT YANG DIKETAHUI MASIH ADA")
    items = []
    for iv in ['5m', '15m', '1h', 'd']:
        n = one(f"SELECT count() FROM {TABLE} WHERE interval=%s "
                f"AND (hour(timestamp) < 2 OR hour(timestamp) > 9)"
                if iv not in ('d',) else
                f"SELECT count() FROM {TABLE} WHERE interval=%s AND hour(timestamp) <> 0",
                (iv,))
        if n:
            items.append((f"jam di luar sesi ({iv})", n, "belum diadjudikasi ke API"))
    n = one(f"SELECT count() FROM {TABLE} WHERE created_at IS NULL")
    tot = one(f"SELECT count() FROM {TABLE}")
    items.append(("created_at kosong", n, f"{n / tot * 100:.1f}% — asal tulisan tak terlacak"))
    n = one(f"SELECT count() FROM {TABLE} WHERE symbol = '__HEALTHCHECK__'")
    if n:
        items.append(("baris uji saya", n, "kontaminasi, terbuang di rebuild berikutnya"))
    n = one("SELECT count() FROM eodhd_stock_metadata") - one(
        "SELECT count() FROM (SELECT symbol, interval FROM eodhd_stock_metadata "
        "GROUP BY symbol, interval)")
    items.append(("metadata berlebih", n, "tabel tanpa DEDUP, terus tumbuh"))

    w = max(len(i[0]) for i in items)
    for label, n, note in sorted(items, key=lambda x: -x[1]):
        print(f"  {label:<{w}}  {n:>10,}  {note}")

    print("\n  Sudah nol:")
    for label, sql in [
        ("bar kosong", f"SELECT count() FROM {TABLE} WHERE open IS NULL AND high IS NULL "
                       f"AND low IS NULL AND close IS NULL AND (volume IS NULL OR volume=0)"),
        ("harga sentinel", f"SELECT count() FROM {TABLE} WHERE close >= {SENTINEL_MIN}"),
        ("interval tak dikenal", f"SELECT count() FROM {TABLE} "
                                 f"WHERE interval NOT IN ('5m','15m','1h','4h','d','w','m')"),
        ("bar w/m berlebih", f"SELECT count() FROM {TABLE} WHERE interval='m'"
                             f" - (SELECT count() FROM (SELECT symbol, date_trunc('month',"
                             f" timestamp) p FROM {TABLE} WHERE interval='m'"
                             f" GROUP BY symbol, p))"),
    ]:
        try:
            print(f"    {label:22s} {one(sql):>10,}")
        except Exception:
            pass


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--no-api', action='store_true')
    args = p.parse_args()
    if not args.no_api:
        accuracy()
    defects()
    print()
