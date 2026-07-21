#!/usr/bin/env python3
"""
Rebuild eodhd_corporate_actions around its business date.

WHY
---
The table was created with `timestamp(created_at)` — the INSERT time — as its
designated timestamp. Two consequences followed:

  * QuestDB requires the designated timestamp to be part of DEDUP UPSERT KEYS, and
    created_at differs on every re-fetch, so the table could never be deduplicated.
    Each daily sweep appended a fresh copy of the entire dividend history:
    584,764 rows holding 4,584 distinct facts (128x).
  * 324,780 of those rows are pure junk from an older code path — a single symbol
    (BRIS.JK) with every date at epoch zero and amount 0.0.

Rebuilding with `timestamp(action_date)` fixes both at once: the junk is dropped and
DEDUP becomes possible, so future re-fetches overwrite instead of accumulating.

VALUE SELECTION
---------------
The duplicates are not all identical: 40 keys carry differing dividend_amount,
because the collector stores EODHD's `value` (adjusted) rather than
`unadjustedValue` (stable), and the adjustment is recomputed over time. We keep
last() — the most recent fetch, hence the current adjustment factor. QuestDB's
last() follows designated-timestamp order, which on the old table is created_at,
so this is precisely "the newest row we ever fetched".

SAFETY
------
The old table is renamed, not dropped, and left in place until the caller passes
--drop-old on a second run. Run a verified backup first.
"""

import argparse
import logging
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_batch

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE, BATCH_INSERT_SIZE,
)

logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

LIVE = 'eodhd_corporate_actions'
NEW = 'eodhd_corporate_actions_v2'
OLD = 'eodhd_corporate_actions_old'

CREATE_NEW = f"""
CREATE TABLE {NEW} (
    symbol SYMBOL,
    action_type SYMBOL,
    action_date TIMESTAMP,
    dividend_amount DOUBLE,
    dividend_currency SYMBOL,
    -- TIMESTAMP, not DATE, for all four date columns: the designated timestamp
    -- must be a TIMESTAMP anyway, and QuestDB refuses to insert a TIMESTAMP value
    -- into a DATE column, which is what reading these back yields.
    payment_date TIMESTAMP,
    record_date TIMESTAMP,
    declaration_date TIMESTAMP,
    dividend_type SYMBOL,
    split_ratio STRING,
    split_from INT,
    split_to INT,
    created_at TIMESTAMP
) timestamp(action_date) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(action_date, symbol, action_type)
"""

# last() over the old table's designated timestamp (created_at) = newest fetch.
EXTRACT = f"""
SELECT symbol, action_type, action_date,
       last(dividend_amount), last(dividend_currency),
       last(payment_date), last(record_date), last(declaration_date),
       last(dividend_type), last(split_ratio), last(split_from), last(split_to),
       last(created_at)
FROM {LIVE}
WHERE year(action_date) > 1970
GROUP BY symbol, action_type, action_date
"""


def connect():
    conn = psycopg2.connect(
        host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
    )
    conn.autocommit = True
    return conn


def table_exists(cur, name):
    cur.execute("SELECT count(*) FROM tables() WHERE table_name = %s", (name,))
    return cur.fetchone()[0] > 0


def rebuild(conn, dry_run: bool):
    cur = conn.cursor()

    cur.execute(f"SELECT count() FROM {LIVE}")
    before = cur.fetchone()[0]
    cur.execute(f"""SELECT count(*) FROM (SELECT symbol, action_type, action_date FROM {LIVE}
                    WHERE year(action_date) > 1970
                    GROUP BY symbol, action_type, action_date)""")
    expected = cur.fetchone()[0]
    cur.execute(f"SELECT count() FROM {LIVE} WHERE year(action_date) = 1970")
    junk = cur.fetchone()[0]

    logger.info(f"Source     : {before:,} rows")
    logger.info(f"  epoch junk to drop : {junk:,}")
    logger.info(f"  distinct facts     : {expected:,}")
    logger.info(f"  reduction          : {before:,} -> {expected:,} ({before/max(expected,1):.0f}x)")

    logger.info("Extracting facts (last() per key = newest fetch)...")
    cur.execute(EXTRACT)
    rows = cur.fetchall()
    logger.info(f"Extracted  : {len(rows):,} rows")

    if len(rows) != expected:
        raise RuntimeError(f"Extraction returned {len(rows):,}, expected {expected:,}")

    if dry_run:
        logger.info("DRY RUN — nothing written. Sample of what would be inserted:")
        for r in rows[:5]:
            logger.info(f"    {r[0]:10s} {r[1]:9s} {str(r[2])[:10]}  amount={r[3]}")
        return

    if table_exists(cur, NEW):
        cur.execute(f"DROP TABLE {NEW}")
    cur.execute(CREATE_NEW)
    time.sleep(1)
    logger.info(f"Created    : {NEW}")

    sql = f"""INSERT INTO {NEW}
        (symbol, action_type, action_date, dividend_amount, dividend_currency,
         payment_date, record_date, declaration_date, dividend_type,
         split_ratio, split_from, split_to, created_at)
        VALUES ({', '.join(['%s'] * 13)})"""
    execute_batch(cur, sql, rows, page_size=BATCH_INSERT_SIZE)
    time.sleep(3)

    cur.execute(f"SELECT count() FROM {NEW}")
    written = cur.fetchone()[0]
    logger.info(f"Written    : {written:,} rows")
    if written != expected:
        raise RuntimeError(f"{NEW} holds {written:,}, expected {expected:,} — NOT swapping")

    # Prove the values survived, not just the row count.
    cur.execute(f"""SELECT count() FROM {NEW} a
                    WHERE a.dividend_amount IS NOT NULL AND a.dividend_amount > 0""")
    with_amount = cur.fetchone()[0]
    cur.execute(f"SELECT count() FROM {NEW} WHERE action_type = 'split'")
    splits = cur.fetchone()[0]
    cur.execute(f"SELECT min(action_date), max(action_date) FROM {NEW}")
    lo, hi = cur.fetchone()
    logger.info(f"  dividends with amount : {with_amount:,}")
    logger.info(f"  splits                : {splits:,}")
    logger.info(f"  date range            : {str(lo)[:10]} .. {str(hi)[:10]}")

    if table_exists(cur, OLD):
        raise RuntimeError(f"{OLD} already exists — resolve it before swapping again")
    cur.execute(f"RENAME TABLE {LIVE} TO {OLD}")
    cur.execute(f"RENAME TABLE {NEW} TO {LIVE}")
    time.sleep(1)
    logger.info(f"Swapped    : {LIVE} is the rebuilt table, previous kept as {OLD}")

    cur.execute(f"SELECT count() FROM {LIVE}")
    logger.info(f"Live now   : {cur.fetchone()[0]:,} rows")
    logger.info(f"Old kept   : re-run with --drop-old once you are satisfied")


def drop_old(conn):
    cur = conn.cursor()
    if not table_exists(cur, OLD):
        logger.info(f"{OLD} does not exist — nothing to drop")
        return
    cur.execute(f"SELECT count() FROM {OLD}")
    n = cur.fetchone()[0]
    cur.execute(f"DROP TABLE {OLD}")
    logger.info(f"Dropped {OLD} ({n:,} rows)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true', help="Report only, write nothing")
    parser.add_argument('--drop-old', action='store_true',
                        help=f"Drop {OLD} left behind by a previous run")
    args = parser.parse_args()

    conn = connect()
    try:
        if args.drop_old:
            drop_old(conn)
        else:
            rebuild(conn, args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
