#!/usr/bin/env python3
"""
Remove the out-of-session rows the API does not recognise — and only those.

Verdicts come from scripts/adjudicate_out_of_session.py, which checked each class
against EODHD rather than against a rule about market hours:

    KEEP   15m hour 1     56,180   the API returns bars at this timestamp
    KEEP   15m hour 10    50,684   the API returns bars at this timestamp
    DROP   5m  0,1,19,20,21,23     12,672
    DROP   15m 0,19,20,21,23        3,823
    DROP   1h  0,1,19,20,21,23      1,345
    DROP   d   hour 17                295

106,864 of the 124,999 out-of-session rows are real. The original plan was to
delete the whole class; doing so would have destroyed every one of them.

The daily 17:00 rows are safe to drop for a different reason: 17:00 UTC is
midnight WIB the following day, and every one of the 295 duplicates a midnight
bar — 233 on the same date, 62 on the next with identical closes, zero orphans.

QuestDB has no DELETE, so this rebuilds and swaps, keeping the previous table.

Usage:
    python scripts/remove_out_of_session_junk.py --dry-run
    python scripts/remove_out_of_session_junk.py --swap
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

SRC = 'eodhd_stock_data'
NEW = 'eodhd_stock_data_oos'
OLD = 'eodhd_stock_data_preoos'

# interval -> hours to drop. Anything not listed stays.
DROP = {
    '5m':  [0, 1, 19, 20, 21, 23],
    '15m': [0, 19, 20, 21, 23],          # 1 and 10 are REAL, deliberately absent
    '1h':  [0, 1, 19, 20, 21, 23],
    'd':   [17],
}

conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)
conn.autocommit = True
cur = conn.cursor()


def one(sql):
    cur.execute(sql)
    r = cur.fetchone()
    return r[0] if r else None


def drop_clause(iv):
    hours = ' OR '.join(f"hour(timestamp) = {h}" for h in DROP[iv])
    return f"interval = '{iv}' AND ({hours})"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--swap', action='store_true')
    args = p.parse_args()

    print(f"{'interval':9s} {'dibuang':>9s}  jam")
    total_drop = 0
    for iv in DROP:
        n = one(f"SELECT count() FROM {SRC} WHERE {drop_clause(iv)}")
        total_drop += n
        print(f"{iv:9s} {n:>9,}  {DROP[iv]}")
    print(f"{'TOTAL':9s} {total_drop:>9,}")

    keep_15m = one(f"SELECT count() FROM {SRC} WHERE interval='15m' "
                   f"AND (hour(timestamp)=1 OR hour(timestamp)=10)")
    print(f"\n  dipertahankan (15m jam 1 & 10, terbukti asli): {keep_15m:,}")

    if args.dry_run:
        print("\nDRY RUN — tidak ada yang ditulis.")
        return

    before = one(f"SELECT count() FROM {SRC}")
    if one(f"SELECT count() FROM tables() WHERE table_name = '{NEW}'"):
        cur.execute(f"DROP TABLE {NEW}")
    cur.execute(f"""
        CREATE TABLE {NEW} (
            symbol SYMBOL CAPACITY 1024 CACHE, interval SYMBOL CAPACITY 16 CACHE,
            timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, adjusted_close DOUBLE, volume LONG,
            gmtoffset INT, source SYMBOL CAPACITY 16 CACHE, created_at TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY MONTH WAL
          DEDUP UPSERT KEYS(timestamp, symbol, interval)
    """)

    where = " AND ".join(f"NOT ({drop_clause(iv)})" for iv in DROP)
    t0 = time.time()
    cur.execute(f"INSERT INTO {NEW} SELECT * FROM {SRC} WHERE {where}")
    print(f"\n  disalin dalam {time.time() - t0:.0f}s")

    # Poll rather than sleep: the WAL applies asynchronously, and a fixed wait
    # once counted 2,100 of 103,492 rows and looked like catastrophic loss.
    prev, stable = -1, 0
    for _ in range(90):
        time.sleep(2)
        now = one(f"SELECT count() FROM {NEW}")
        stable = stable + 1 if now == prev else 0
        prev = now
        if stable >= 3:
            break
    after = prev
    expected = before - total_drop
    print(f"  {SRC}: {before:,}")
    print(f"  {NEW}: {after:,}  (diharapkan {expected:,}, selisih {after - expected:+,})")

    checks_ok = after == expected
    for iv in DROP:
        left = one(f"SELECT count() FROM {NEW} WHERE {drop_clause(iv)}")
        print(f"  {iv}: sisa baris yang harusnya hilang = {left}")
        checks_ok &= (left == 0)
    kept = one(f"SELECT count() FROM {NEW} WHERE interval='15m' "
               f"AND (hour(timestamp)=1 OR hour(timestamp)=10)")
    print(f"  15m jam 1 & 10 masih ada: {kept:,}")
    checks_ok &= (kept == keep_15m)

    if not checks_ok:
        print("\n  PEMERIKSAAN GAGAL — jangan tukar nama.")
        sys.exit(1)
    print("\n  Semua pemeriksaan lulus.")

    if args.swap:
        cur.execute(f"RENAME TABLE {SRC} TO {OLD}")
        cur.execute(f"RENAME TABLE {NEW} TO {SRC}")
        print(f"  Ditukar. Tabel sebelumnya: {OLD}")
    else:
        print("  Jalankan ulang dengan --swap untuk menukar nama.")


if __name__ == "__main__":
    main()
