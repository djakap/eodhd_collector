#!/usr/bin/env python3
"""
Verify eodhd_stock_data_v2 before its name is swapped in, and regenerate 4h.

The twin is only trustworthy if every row that should have survived did, with the
same values. Counting alone is not enough — an earlier build matched on intraday
and was 17,699 short on w/m, because converting sentinel prices to NULL turned
rows into "empty" ones that the write gate then dropped. That was found by
comparing counts per interval, so this checks per interval and then per row.

Read-only against the source. Writes only the regenerated 4h into the twin.

Usage:
    python scripts/verify_rebuild.py            # verify + regenerate 4h
    python scripts/verify_rebuild.py --swap     # also rename, after all checks pass
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
from utils.bar_rules import SENTINEL_MIN

SRC, NEW, OLD = 'eodhd_stock_data', 'eodhd_stock_data_v2', 'eodhd_stock_data_pre_cleanup'
EMPTY = "open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL"
WILL_BE_EMPTY = (
    f"open >= {SENTINEL_MIN} AND high >= {SENTINEL_MIN} "
    f"AND low >= {SENTINEL_MIN} AND close >= {SENTINEL_MIN} "
    f"AND (volume IS NULL OR volume = 0)"
)

conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)
conn.autocommit = True
cur = conn.cursor()
failures = []


def one(sql):
    cur.execute(sql)
    r = cur.fetchone()
    return r[0] if r else None


def check(label, ok, detail=""):
    print(f"  {'OK  ' if ok else 'GAGAL'}  {label:52s} {detail}")
    if not ok:
        failures.append(label)


print("=" * 76)
print("1. YANG SEHARUSNYA HILANG, SUDAH HILANG")
print("=" * 76)
check("30m tidak ada", one(f"SELECT count() FROM {NEW} WHERE interval='30m'") == 0)
check("4h_null tidak ada", one(f"SELECT count() FROM {NEW} WHERE interval='4h_null'") == 0)
check("tidak ada bar kosong",
      one(f"SELECT count() FROM {NEW} WHERE {EMPTY} AND (volume IS NULL OR volume=0)") == 0)
n_sent = one(f"SELECT count() FROM {NEW} WHERE open>={SENTINEL_MIN} "
             f"OR high>={SENTINEL_MIN} OR low>={SENTINEL_MIN} OR close>={SENTINEL_MIN}")
check("tidak ada harga sentinel", n_sent == 0, f"({n_sent} tersisa)")
n_contam = one(f"SELECT count() FROM {NEW} WHERE interval='1h' "
               f"AND timestamp >= '2026-07-01' "
               f"AND symbol IN ('BRIS.JK','ANTM.JK','TLKM.JK') "
               f"AND (hour(timestamp) < 2 OR hour(timestamp) > 9)")
check("kontaminasi 1h Juli hilang", n_contam == 0, f"({n_contam} tersisa)")

print("\n" + "=" * 76)
print("2. YANG SEHARUSNYA BERTAHAN, MASIH ADA — per interval")
print("=" * 76)
print(f"  {'iv':6s} {'sumber non-kosong':>18s} {'v2':>12s} {'selisih':>10s}")
for iv, expect_drop in [('5m', 0), ('15m', 0), ('1h', 114), ('d', 0),
                        ('w', 2228), ('m', 40804)]:
    # Mirror the rebuild's own predicate rather than hardcoding a number: rows
    # whose every price is the sentinel AND that carry no volume end up empty once
    # repaired, so they never reach the twin. Eight daily rows qualify.
    src = one(f"SELECT count() FROM {SRC} WHERE interval='{iv}' "
              f"AND NOT ({EMPTY}) AND NOT ({WILL_BE_EMPTY})")
    new = one(f"SELECT count() FROM {NEW} WHERE interval='{iv}'")
    diff = new - (src - expect_drop)
    print(f"  {iv:6s} {src:>18,} {new:>12,} {diff:>+10,}")
    check(f"{iv}: jumlah sesuai", diff == 0)

print("\n" + "=" * 76)
print("3. NILAI COCOK BARIS PER BARIS — sampel acak")
print("=" * 76)
cur.execute(f"""SELECT symbol, interval, timestamp, open, high, low, close, volume
                FROM {SRC} WHERE interval IN ('5m','1h','d')
                AND NOT ({EMPTY}) AND close < {SENTINEL_MIN}
                LIMIT 400""")
sample = cur.fetchall()
mismatch = 0
for r in sample:
    cur.execute(f"""SELECT open, high, low, close, volume FROM {NEW}
                    WHERE symbol=%s AND interval=%s AND timestamp=%s""",
                (r[0], r[1], r[2]))
    got = cur.fetchone()
    if got is None or any(
            (a is None) != (b is None) or
            (a is not None and abs(float(a) - float(b)) > 1e-9)
            for a, b in zip(r[3:], got)):
        mismatch += 1
check(f"{len(sample)} baris sampel identik", mismatch == 0, f"({mismatch} beda)")

print("\n" + "=" * 76)
print("4. CAKUPAN SIMBOL DAN RENTANG TANGGAL")
print("=" * 76)
for iv in ['5m', '15m', '1h', 'd', 'w', 'm']:
    s_sym = one(f"SELECT count_distinct(symbol) FROM {SRC} WHERE interval='{iv}' AND NOT ({EMPTY}) AND NOT ({WILL_BE_EMPTY})")
    n_sym = one(f"SELECT count_distinct(symbol) FROM {NEW} WHERE interval='{iv}'")
    s_min = one(f"SELECT min(timestamp) FROM {SRC} WHERE interval='{iv}' AND NOT ({EMPTY}) AND NOT ({WILL_BE_EMPTY})")
    n_min = one(f"SELECT min(timestamp) FROM {NEW} WHERE interval='{iv}'")
    s_max = one(f"SELECT max(timestamp) FROM {SRC} WHERE interval='{iv}' AND NOT ({EMPTY}) AND NOT ({WILL_BE_EMPTY})")
    n_max = one(f"SELECT max(timestamp) FROM {NEW} WHERE interval='{iv}'")
    ok = (s_sym == n_sym and s_min == n_min and s_max == n_max)
    print(f"  {iv:5s} simbol {s_sym}->{n_sym}  rentang {str(s_min)[:10]}..{str(s_max)[:10]}"
          f" -> {str(n_min)[:10]}..{str(n_max)[:10]}")
    check(f"{iv}: cakupan utuh", ok)

print("\n" + "=" * 76)
print("5. REGENERASI 4h DARI 1h BERSIH")
print("=" * 76)
existing = one(f"SELECT count() FROM {NEW} WHERE interval='4h'")
if existing:
    print(f"  4h sudah ada ({existing:,}) — dilewati")
else:
    t0 = time.time()
    # Same session assignment as utils/aggregate_4h, which is kept as-is: the
    # morning bar covers 02:00-05:00 UTC and the afternoon one 06:00-09:00.
    cur.execute(f"""
        INSERT INTO {NEW}
        SELECT symbol, '4h' as interval,
            CASE WHEN hour(timestamp) IN (2,3,4)
                    THEN dateadd('h', 2, date_trunc('day', timestamp))
                 WHEN hour(timestamp) IN (6,7,8,9)
                    THEN dateadd('h', 6, date_trunc('day', timestamp))
            END as ts,
            first(open), max(high), min(low), last(close), last(adjusted_close),
            sum(volume), 0, 'aggregated', now()
        FROM {NEW}
        WHERE interval = '1h' AND hour(timestamp) IN (2,3,4,6,7,8,9)
          AND open IS NOT NULL
        GROUP BY symbol,
            CASE WHEN hour(timestamp) IN (2,3,4)
                    THEN dateadd('h', 2, date_trunc('day', timestamp))
                 WHEN hour(timestamp) IN (6,7,8,9)
                    THEN dateadd('h', 6, date_trunc('day', timestamp))
            END
    """)
    time.sleep(8)
    made = one(f"SELECT count() FROM {NEW} WHERE interval='4h'")
    print(f"  {made:,} bar 4h dibuat dalam {time.time() - t0:.0f}s")
    check("4h terbentuk", made > 0)
    check("4h tidak punya bar kosong",
          one(f"SELECT count() FROM {NEW} WHERE interval='4h' AND {EMPTY}") == 0)

print("\n" + "=" * 76)
total_new = one(f"SELECT count() FROM {NEW}")
total_src = one(f"SELECT count() FROM {SRC}")
print(f"  {SRC:28s} {total_src:>12,}")
print(f"  {NEW:28s} {total_new:>12,}   ({total_new - total_src:+,})")

if failures:
    print(f"\n  {len(failures)} PEMERIKSAAN GAGAL — JANGAN TUKAR:")
    for f in failures:
        print(f"    - {f}")
    sys.exit(1)

print("\n  SEMUA PEMERIKSAAN LULUS.")

args = argparse.ArgumentParser()
args.add_argument('--swap', action='store_true')
opts = args.parse_args()
if opts.swap:
    print(f"\n  Menukar nama: {SRC} -> {OLD}, {NEW} -> {SRC}")
    cur.execute(f"RENAME TABLE {SRC} TO {OLD}")
    cur.execute(f"RENAME TABLE {NEW} TO {SRC}")
    print(f"  Selesai. Tabel lama tersimpan sebagai {OLD} — JANGAN dihapus")
    print(f"  sampai beberapa hari berjalan normal.")
else:
    print(f"  Jalankan ulang dengan --swap untuk menukar nama.")
