#!/usr/bin/env python3
"""
Promote yf_stock_data to the production table `stock_data`.

The migration decision: yfinance becomes the single source, cut off today, all
timeframes we use (1h/d/w/m). yf_stock_data already holds that — 4.1M rows, 651
symbols, current to today, split-adjusted and clean (0 empty, 0 sentinel). This
makes it canonical under a name that no longer carries a source prefix, because it
is simply THE data now.

Built as a fresh table + copy rather than a bare RENAME so one blemish can be
fixed in the same pass: SMCB carries two weekly bars in ISO weeks 2026-28 and -29
(Monday label and Tuesday label, identical close), the Monday-holiday version of
the restamp problem. Weekly keeps the earliest label per (symbol, ISO week);
everything else copies straight.

IIKP and SCPI have daily history but no monthly, and never will — yfinance returns
nothing for them now (delisted/suspended). That is left as-is.

yf_stock_data is kept until the new table is verified and the collectors are
repointed, then it can be dropped.

Usage:
    python scripts/promote_to_production.py --dry-run
    python scripts/promote_to_production.py --swap
"""

import argparse
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)

SRC = 'yf_stock_data'
NEW = 'stock_data'

conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)
conn.autocommit = True
cur = conn.cursor()


def one(sql, params=None):
    cur.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None


def weekly_losers():
    """The later-labelled bar in any (symbol, ISO week) that holds more than one."""
    cur.execute(f"SELECT symbol, timestamp FROM {SRC} WHERE interval='w'")
    groups = defaultdict(list)
    for s, ts in cur.fetchall():
        iso = ts.isocalendar()
        groups[(s, iso[0], iso[1])].append(ts)
    losers = set()
    for (sym, _, _), stamps in groups.items():
        if len(stamps) > 1:
            keep = min(stamps)
            for ts in stamps:
                if ts != keep:
                    losers.add((sym, ts))
    return losers


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--swap', action='store_true')
    args = p.parse_args()

    losers = weekly_losers()
    before = one(f"SELECT count() FROM {SRC}")
    print(f"{SRC}: {before:,} baris")
    print(f"duplikat mingguan yang dibuang: {len(losers)}  {sorted((s,str(t)[:10]) for s,t in losers)}")
    print(f"target {NEW}: {before - len(losers):,} baris")

    if args.dry_run:
        print("\nDRY RUN — tidak ada yang ditulis.")
        return

    if one(f"SELECT count() FROM tables() WHERE table_name='{NEW}'"):
        print(f"\n{NEW} sudah ada — dihapus dan dibuat ulang")
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
    print(f"{NEW} dibuat")

    # Everything except weekly copies straight.
    t0 = time.time()
    cur.execute(f"INSERT INTO {NEW} SELECT * FROM {SRC} WHERE interval <> 'w'")
    print(f"  non-mingguan disalin dalam {time.time() - t0:.0f}s")

    # Weekly, minus the duplicate labels.
    from db.questdb_client import QuestDBClient
    cur.execute(f"""SELECT symbol, interval, timestamp, open, high, low, close,
                    adjusted_close, volume, gmtoffset, source, created_at
                    FROM {SRC} WHERE interval='w'""")
    rows = [r for r in cur.fetchall() if (r[0], r[2]) not in losers]
    db = QuestDBClient()
    db.connect()
    try:
        for i in range(0, len(rows), 5000):
            db.insert_price_data(rows[i:i + 5000], table=NEW)
    finally:
        db.close()
    print(f"  mingguan disalin: {len(rows):,}")

    # WAL applies asynchronously — poll until the count settles.
    prev, stable = -1, 0
    for _ in range(90):
        time.sleep(2)
        now = one(f"SELECT count() FROM {NEW}")
        stable = stable + 1 if now == prev else 0
        prev = now
        if stable >= 3:
            break
    after = prev
    expected = before - len(losers)
    print(f"\n  {NEW}: {after:,}  (diharapkan {expected:,}, selisih {after - expected:+,})")

    ok = after == expected
    for iv, unit in (('m', 'month'), ('w', 'week')):
        tot = one(f"SELECT count() FROM {NEW} WHERE interval='{iv}'")
        uq = one(f"SELECT count() FROM (SELECT symbol, date_trunc('{unit}', timestamp) p "
                 f"FROM {NEW} WHERE interval='{iv}' GROUP BY symbol, p)")
        print(f"  {iv}: {tot:,} baris / {uq:,} periode -> {tot - uq:,} duplikat")
        ok &= (tot == uq)
    for iv in ['1h', 'd', 'w', 'm']:
        s = one(f"SELECT count_distinct(symbol) FROM {NEW} WHERE interval='{iv}'")
        print(f"  {iv}: {s} simbol")

    if not ok:
        print("\n  PEMERIKSAAN GAGAL — jangan promosikan.")
        sys.exit(1)
    print(f"\n  {NEW} bersih dan siap. {SRC} dipertahankan sebagai cadangan.")
    print(f"  Berikutnya: arahkan config kolektor ke '{NEW}', rebuild worker.")


if __name__ == "__main__":
    main()
