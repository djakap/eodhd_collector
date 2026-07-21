#!/usr/bin/env python3
"""
Rebuild eodhd_stock_data without the rows the audit and the API reconciliation
proved to be junk.

QuestDB 7.3.10 has no DELETE, and the bad rows sit interleaved with good ones
across 443 monthly partitions, so DROP PARTITION would take valid data with them.
The only way is to build a clean twin and swap names, keeping the original until
the replacement is verified.

WHAT IS REMOVED, and the evidence for each:

  30m, 3,053,447 rows
      Not needed, and redundant: deriving 30m from stored 5m reproduced EODHD's
      native 30m exactly on every healthy date tested (179 buckets, 0 differences),
      the same relationship 4h already has with 1h. Its own timestamps are also the
      worst in the table — on 2024-11-12 the 30m series starts at 04:00 UTC while
      5m and the API start at 02:00.

  empty bars, 3,875,539 rows
      All four OHLC fields NULL. Not one carries volume, checked across the whole
      table. Removing them is what actually repairs the months that looked
      catastrophic: 2024-11 read as 62% damaged, but after the empties come out the
      remainder matches EODHD bar for bar. Verified on 240 symbol/date/interval
      cells spanning the full archive — 240 matches, 0 mismatches.

  4h_null, 17,321 rows
      An interval name that never existed, every field NULL, written by an older
      aggregate_4h. The current one cannot produce it.

  114 rows of my own contamination
      1h bars for BRIS/ANTM/TLKM stamped outside session hours in July 2026,
      written from the host while testing before the ILP timezone fix landed.
      yfinance and the EODHD API both show nothing at those hours.

  43,032 surplus w/m rows
      Periods holding more than one bar. EODHD returns exactly one, and the
      manifest from scripts/reconcile_periods.py records which stored rows the API
      does not recognise. Adjudicated per row against the source, never by a local
      rule — testing date_trunc('month') instead flagged 84,672 VALID rows, because
      EODHD stamps a monthly bar on the first TRADING day.

WHAT IS REPAIRED RATHER THAN REMOVED:

  72,092 sentinel prices
      EODHD writes 999999.9999 where it has no price. Stored as a price, it makes
      ASII in 1994 look like a million-rupiah stock. But 53,902 of the 53,910 daily
      ones carry a real volume, so the row is not empty — only the price is absent.
      The price fields become NULL and the row stays.

The remaining 1,345 out-of-session 1h rows are deliberately NOT touched. They
predate the contamination and have not been checked against the API, and this
whole effort has repeatedly shown that deleting unverified rows is how good data
dies. The daily audit will keep reporting them until someone adjudicates them.

Usage:
    python scripts/rebuild_stock_data.py --dry-run      # counts only, no writes
    python scripts/rebuild_stock_data.py                # build the twin
    python scripts/rebuild_stock_data.py --swap         # rename after verifying
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)
from utils.bar_rules import SENTINEL_MIN

SRC = 'eodhd_stock_data'
NEW = 'eodhd_stock_data_v2'
OLD = 'eodhd_stock_data_pre_cleanup'

KEEP_INTERVALS = ['5m', '15m', '1h', 'd']      # 4h regenerated, 30m dropped
PERIOD_INTERVALS = ['w', 'm']

EMPTY = "open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL"

# A row whose every price is the sentinel becomes all-NULL once repaired. If it has
# no volume either, it ends up carrying nothing at all — so it has to be excluded
# here, where the values are still the originals. The SQL filter runs BEFORE the
# CASE that rewrites them, which is how 8 daily rows slipped into the first build.
WILL_BE_EMPTY = (
    f"open >= {SENTINEL_MIN} AND high >= {SENTINEL_MIN} "
    f"AND low >= {SENTINEL_MIN} AND close >= {SENTINEL_MIN} "
    f"AND (volume IS NULL OR volume = 0)"
)

# My own contamination: 1h bars written from the host during testing, before the
# ILP timezone fix. Only these are removed on the out-of-session grounds.
CONTAMINATION = (
    "interval = '1h' AND timestamp >= '2026-07-01' "
    "AND symbol IN ('BRIS.JK','ANTM.JK','TLKM.JK') "
    "AND (hour(timestamp) < 2 OR hour(timestamp) > 9)"
)


def price(col):
    """Sentinel prices become NULL; everything else passes through."""
    return f"CASE WHEN {col} >= {SENTINEL_MIN} THEN NULL ELSE {col} END"


class Rebuild:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
            password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def one(self, sql):
        self.cur.execute(sql)
        r = self.cur.fetchone()
        return r[0] if r else None

    def exists(self, table):
        self.cur.execute("SELECT count() FROM tables() WHERE table_name = %s", (table,))
        return self.cur.fetchone()[0] > 0

    # ------------------------------------------------------------------
    def plan(self, surplus_keys):
        print(f"{'interval':10s} {'sekarang':>12s} {'kosong':>10s} "
              f"{'surplus':>9s} {'dipertahankan':>14s}")
        total_src = total_keep = 0
        for iv in KEEP_INTERVALS + PERIOD_INTERVALS + ['4h', '30m', '4h_null']:
            n = self.one(f"SELECT count() FROM {SRC} WHERE interval = '{iv}'")
            if not n:
                continue
            total_src += n
            if iv in ('30m', '4h_null'):
                print(f"{iv:10s} {n:>12,} {'—':>10s} {'—':>9s} "
                      f"{0:>14,}  <-- dibuang seluruhnya")
                continue
            if iv == '4h':
                print(f"{iv:10s} {n:>12,} {'—':>10s} {'—':>9s} "
                      f"{'regenerasi':>14s}")
                continue
            empty = self.one(f"SELECT count() FROM {SRC} WHERE interval = '{iv}' "
                             f"AND (({EMPTY}) OR ({WILL_BE_EMPTY}))")
            surp = len(surplus_keys.get(iv, ()))
            keep = n - empty - surp
            if iv == '1h':
                contam = self.one(f"SELECT count() FROM {SRC} WHERE {CONTAMINATION}")
                keep -= contam
            total_keep += keep
            print(f"{iv:10s} {n:>12,} {empty:>10,} {surp:>9,} {keep:>14,}")
        print(f"{'TOTAL':10s} {total_src:>12,} {'':>10s} {'':>9s} {total_keep:>14,}"
              f"   (+ 4h regenerasi)")
        return total_keep

    # ------------------------------------------------------------------
    def create_twin(self):
        if self.exists(NEW):
            print(f"  {NEW} sudah ada — dihapus dan dibuat ulang")
            self.cur.execute(f"DROP TABLE {NEW}")
        self.cur.execute(f"""
            CREATE TABLE {NEW} (
                symbol SYMBOL CAPACITY 1024 CACHE,
                interval SYMBOL CAPACITY 16 CACHE,
                timestamp TIMESTAMP,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                adjusted_close DOUBLE, volume LONG,
                gmtoffset INT, source SYMBOL CAPACITY 16 CACHE,
                created_at TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY MONTH WAL
              DEDUP UPSERT KEYS(timestamp, symbol, interval)
        """)
        print(f"  {NEW} dibuat")

    def copy_interval(self, iv, extra_where=""):
        cols = (f"symbol, interval, timestamp, {price('open')}, {price('high')}, "
                f"{price('low')}, {price('close')}, {price('adjusted_close')}, "
                f"volume, gmtoffset, source, created_at")
        where = (f"interval = '{iv}' AND NOT ({EMPTY}) "
                 f"AND NOT ({WILL_BE_EMPTY})")
        if extra_where:
            where += f" AND NOT ({extra_where})"
        t0 = time.time()
        self.cur.execute(f"INSERT INTO {NEW} SELECT {cols} FROM {SRC} WHERE {where}")
        print(f"    {iv:5s} disalin dalam {time.time() - t0:.0f}s")

    def copy_periods(self, iv, surplus):
        """
        w/m are filtered per row against the reconciliation manifest, which SQL
        cannot express here: the exclusion is a set of 43,032 (symbol, date) pairs
        adjudicated against EODHD's API, not a predicate. At 582k rows for both
        intervals combined, pulling them through Python costs little.
        """
        from db.questdb_client import QuestDBClient

        t0 = time.time()
        self.cur.execute(
            f"SELECT symbol, interval, timestamp, open, high, low, close, "
            f"adjusted_close, volume, gmtoffset, source, created_at "
            f"FROM {SRC} WHERE interval = '{iv}' AND NOT ({EMPTY})")
        rows = self.cur.fetchall()

        def clean(v):
            return None if v is not None and float(v) >= SENTINEL_MIN else v

        kept, dropped = [], 0
        for r in rows:
            if (r[0], str(r[2])[:10]) in surplus:
                dropped += 1
                continue
            kept.append((r[0], r[1], r[2], clean(r[3]), clean(r[4]), clean(r[5]),
                         clean(r[6]), clean(r[7]), r[8], r[9], r[10], r[11]))

        db = QuestDBClient()
        db.connect()
        try:
            for i in range(0, len(kept), 5000):
                db.insert_price_data(kept[i:i + 5000], table=NEW)
        finally:
            db.close()

        print(f"    {iv:5s} {len(rows):>8,} dibaca, {dropped:>6,} surplus dibuang, "
              f"{len(kept):>8,} ditulis dalam {time.time() - t0:.0f}s")
        return len(kept)


def load_surplus(path):
    """Manifest -> {interval: {(symbol, 'YYYY-MM-DD'), ...}}"""
    if not os.path.exists(path):
        return {}
    data = json.load(open(path))
    out = {}
    for row in data.get('surplus', []):
        out.setdefault(row['interval'], set()).add((row['symbol'], row['date']))
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--manifest', default='reports/period_reconciliation.json')
    args = p.parse_args()

    surplus = load_surplus(args.manifest)
    print(f"Manifest surplus: " +
          ', '.join(f"{k}={len(v):,}" for k, v in surplus.items()) or "(kosong)")
    print()

    r = Rebuild()
    keep = r.plan(surplus)

    if args.dry_run:
        print("\nDRY RUN — tidak ada yang ditulis.")
        return

    print(f"\nMembangun {NEW} …")
    r.create_twin()
    for iv in KEEP_INTERVALS:
        r.copy_interval(iv, CONTAMINATION if iv == '1h' else "")
    for iv in PERIOD_INTERVALS:
        r.copy_periods(iv, surplus.get(iv, set()))

    time.sleep(5)      # let the WAL apply before counting
    got = r.one(f"SELECT count() FROM {NEW}")
    print(f"\n  {NEW}: {got:,} baris (target {keep:,}, selisih {got - keep:+,})")
    print(f"\n  Tabel lama BELUM disentuh. Jalankan verifikasi sebelum menukar nama.")


if __name__ == "__main__":
    main()
