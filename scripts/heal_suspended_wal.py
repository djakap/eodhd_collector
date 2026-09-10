#!/usr/bin/env python3
"""
Detect and auto-recover suspended QuestDB WAL tables.

Why this exists
---------------
On this laptop/WSL2 host, an unclean shutdown (the laptop sleeping or losing
power mid-write) can leave a QuestDB WAL segment half-written: its `_event.i`
index file ends up empty/truncated (indexFileSize=0, maxTxn=0). When the
ApplyWal2TableJob reaches that segment it fails and SUSPENDS the table:

    C ApplyWal2TableJob job failed, table suspended
      [table=stock_data~94, error=segment .../wal45/0/_event.i does not have txn ...]

A suspended table silently stops applying its WAL — every subsequent write piles
up un-applied, so queries freeze at the last-applied timestamp. On 2026-08-03
this had frozen stock_data at 2026-06-30 with ~2,600 un-applied transactions
(a month of data invisible) while everything looked "up". A plain
`ALTER TABLE t RESUME WAL` just re-hits the same corrupt segment and re-suspends.

What this does
--------------
For every suspended table it first tries a plain RESUME. If that doesn't clear
it, the next transaction to apply (writerTxn+1) sits in a corrupt segment, so it
uses wal_transactions() to find that segment's boundary and RESUMEs FROM the
first transaction of the NEXT segment — skipping exactly the corrupt segment and
nothing more. It repeats until the table is healthy or no progress is made.

The transactions inside a corrupt segment are unreadable and are lost, but for
this pipeline every table is idempotently re-fetchable from source (stock_data
via the intraday/price backfill, corporate_actions via yfinance-actions,
fundamentals via fundamentals-sweep), and the intraday lookback+DEDUP usually
backfills the small stock_data hole on its own. Losing a corrupt segment to get
the table live again is the right trade.

Run at worker startup (alongside clear_orphan_runs) and/or from the nightly
check. Safe to run when nothing is suspended — it just reports and exits 0.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)

SETTLE_SECONDS = 4      # give the apply job a moment to advance or re-suspend
MAX_SKIPS = 25          # cap corrupt segments skipped per table (avoid looping)


def _conn():
    c = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                         password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)
    c.autocommit = True
    return c


def _state(cur, table):
    cur.execute("SELECT suspended, writerTxn, sequencerTxn "
                f"FROM wal_tables() WHERE name = '{table}'")
    return cur.fetchone()          # (suspended, writerTxn, sequencerTxn)


def _next_segment_txn(cur, table, writer_txn, seq_txn):
    """First sequencerTxn of the segment AFTER the one holding writer_txn+1.

    That later segment is where apply can resume once the corrupt segment (the
    one containing the stuck txn) is skipped. If the stuck segment is the last
    one, return seq_txn+1 so we skip to the tip and let new writes flow."""
    cur.execute(
        f"SELECT walId, min(sequencerTxn) mn FROM wal_transactions('{table}') "
        f"WHERE sequencerTxn > {writer_txn} GROUP BY walId ORDER BY mn")
    segs = cur.fetchall()          # [(walId, firstTxn), ...] stuck segment first
    if len(segs) >= 2:
        return segs[1][1]
    return seq_txn + 1


def heal() -> int:
    """Recover every suspended WAL table. Returns the number healed."""
    healed = 0
    with _conn() as c:
        cur = c.cursor()
        cur.execute("SELECT name FROM wal_tables() WHERE suspended = true")
        tables = [r[0] for r in cur.fetchall()]
        if not tables:
            print("no suspended WAL tables")
            return 0

        for t in tables:
            print(f"healing {t} ...")
            cur.execute(f"ALTER TABLE {t} RESUME WAL")   # plain resume first
            time.sleep(SETTLE_SECONDS)

            for _ in range(MAX_SKIPS):
                suspended, w, seq = _state(cur, t)
                if not suspended:
                    print(f"  {t}: healthy (writerTxn={w}, lag={seq - w})")
                    healed += 1
                    break
                skip_to = _next_segment_txn(cur, t, w, seq)
                if skip_to <= w + 1:                     # no forward progress possible
                    print(f"  {t}: STUCK at writerTxn={w}, cannot advance — manual check")
                    break
                print(f"  {t}: corrupt segment at txn {w + 1}; skipping to {skip_to}")
                cur.execute(f"ALTER TABLE {t} RESUME WAL FROM TRANSACTION {skip_to}")
                time.sleep(SETTLE_SECONDS)
            else:
                print(f"  {t}: gave up after {MAX_SKIPS} skips")
    print(f"suspended tables healed: {healed}")
    return healed


if __name__ == "__main__":
    # Non-zero exit only if something is still suspended after healing, so a
    # caller/monitor can alert on a genuinely stuck table.
    heal()
    with _conn() as c:
        cur = c.cursor()
        cur.execute("SELECT count() FROM wal_tables() WHERE suspended = true")
        still = cur.fetchone()[0]
    sys.exit(1 if still else 0)
