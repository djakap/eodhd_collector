#!/usr/bin/env python3
"""
Collapse weekly/monthly periods that hold more than one bar.

Needed because scripts/fill_missing_periods.py checked whether a DATE was present
rather than whether the PERIOD was, and EODHD restamps a bar once its period is
final. ITMA's June 2026 bar sat at 2026-06-24; the API now labels it 2026-06-01
with the same close of 1325. The fill saw the second label as missing and added
it, leaving 651 symbols with two June bars carrying identical data.

Which row survives: the EARLIEST label in each period, because that is the bar
covering the whole period. ITMA at 2026-06-24 opens 1540 on 186,600 volume while
2026-06-01 opens 1750 on 2,604,200 — the later label is a partial month. Ranking
on created_at was tried first and would have kept the partial bar, since both rows
carry today's timestamp and recency says nothing about which is correct.

QuestDB has no DELETE, so removing rows means rebuilding the table and swapping
names. The previous table is kept until the result is checked.

Usage:
    python scripts/dedup_periods.py --dry-run
    python scripts/dedup_periods.py
    python scripts/dedup_periods.py --swap
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

SRC = 'eodhd_stock_data'
NEW = 'eodhd_stock_data_dd'
OLD = 'eodhd_stock_data_predupe'
PERIODS = ('w', 'm')


def period_of(interval, ts):
    if interval == 'm':
        return (ts.year, ts.month)
    iso = ts.isocalendar()
    return (iso[0], iso[1])


conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)
conn.autocommit = True
cur = conn.cursor()


def one(sql, params=None):
    cur.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None


def find_losers():
    """
    Timestamps to drop, as {(symbol, interval, timestamp)}.

    Keep the EARLIEST-labelled bar in each period, because that is the one that
    covers the whole period. The duplicates arose from EODHD restamping a bar once
    its month closed: last night the API returned ITMA's June bar as 2026-06-24
    and today it returns 2026-06-01. Both reconciliations were right at the time,
    and the second added a row beside the first.

    The two are not the same bar. The later label is a PARTIAL period — ITMA at
    2026-06-24 opens 1540 on 186,600 volume, while 2026-06-01 opens 1750 on
    2,604,200. Same close, because both end at month end. Earliest-label wins on
    content, not on which write happened to be newer.

    An earlier version of this ranked on created_at and would have kept the
    partial bar: both rows carry today's timestamp, so recency said nothing about
    which one is right.
    """
    losers = set()
    for iv in PERIODS:
        cur.execute(f'SELECT symbol, timestamp FROM "{SRC}" WHERE interval = %s', (iv,))
        groups = defaultdict(list)
        for s, ts in cur.fetchall():
            groups[(s, period_of(iv, ts))].append(ts)
        for (sym, _), stamps in groups.items():
            if len(stamps) < 2:
                continue
            keep = min(stamps)
            for ts in stamps:
                if ts != keep:
                    losers.add((sym, iv, ts))
    return losers


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--swap', action='store_true')
    args = p.parse_args()

    losers = find_losers()
    print(f"Baris yang akan dibuang: {len(losers):,}")
    by_iv = defaultdict(int)
    for _, iv, _ in losers:
        by_iv[iv] += 1
    for iv, n in sorted(by_iv.items()):
        print(f"  {iv}: {n:,}")
    for s, iv, ts in sorted(losers)[:5]:
        print(f"    contoh: {s:10s} {iv} {str(ts)[:10]}")

    if args.dry_run:
        print("\nDRY RUN — tidak ada yang ditulis.")
        return
    if not losers:
        print("\nTidak ada duplikat. Selesai.")
        return

    total_before = one(f'SELECT count() FROM "{SRC}"')
    if one("SELECT count() FROM tables() WHERE table_name = %s", (NEW,)):
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

    # Everything except w/m copies straight across.
    t0 = time.time()
    cur.execute(f"INSERT INTO {NEW} SELECT * FROM {SRC} "
                f"WHERE interval NOT IN ('w','m')")
    print(f"\n  interval lain disalin dalam {time.time() - t0:.0f}s")

    from db.questdb_client import QuestDBClient
    db = QuestDBClient()
    db.connect()
    try:
        for iv in PERIODS:
            cur.execute(f"""SELECT symbol, interval, timestamp, open, high, low,
                            close, adjusted_close, volume, gmtoffset, source,
                            created_at FROM "{SRC}" WHERE interval = %s""", (iv,))
            rows = [r for r in cur.fetchall() if (r[0], iv, r[2]) not in losers]
            for i in range(0, len(rows), 5000):
                db.insert_price_data(rows[i:i + 5000], table=NEW)
            print(f"  {iv}: {len(rows):,} baris disalin")
    finally:
        db.close()

    # The WAL applies asynchronously. A fixed sleep was not enough — an 8-second
    # wait counted 2,100 of 103,492 monthly rows and the guard below correctly
    # refused to swap on a table that was merely still filling. Poll until the
    # count stops moving instead of guessing how long it takes.
    prev, stable = -1, 0
    for _ in range(60):
        time.sleep(2)
        now = one(f'SELECT count() FROM "{NEW}"')
        stable = stable + 1 if now == prev else 0
        prev = now
        if stable >= 3:
            break
    total_after = prev
    expected = total_before - len(losers)
    print(f"\n  {SRC}: {total_before:,}")
    print(f"  {NEW}: {total_after:,}  (diharapkan {expected:,}, "
          f"selisih {total_after - expected:+,})")

    for iv, unit in (('m', 'month'), ('w', 'week')):
        tot = one(f"SELECT count() FROM {NEW} WHERE interval='{iv}'")
        uniq = one(f"SELECT count() FROM (SELECT symbol, "
                   f"date_trunc('{unit}', timestamp) p FROM {NEW} "
                   f"WHERE interval='{iv}' GROUP BY symbol, p)")
        print(f"  {iv}: {tot:,} baris / {uniq:,} periode -> {tot - uniq:,} duplikat")

    if total_after != expected:
        print("\n  JUMLAH TIDAK COCOK — jangan tukar nama.")
        sys.exit(1)

    if args.swap:
        cur.execute(f"RENAME TABLE {SRC} TO {OLD}")
        cur.execute(f"RENAME TABLE {NEW} TO {SRC}")
        print(f"\n  Ditukar. Tabel sebelumnya tersimpan sebagai {OLD}.")
    else:
        print(f"\n  Jalankan ulang dengan --swap untuk menukar nama.")


if __name__ == "__main__":
    main()
