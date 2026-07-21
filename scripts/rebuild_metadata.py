#!/usr/bin/env python3
"""
Compact eodhd_metadata to one row per symbol.

WHY
---
eodhd_metadata is a current-state table — name, sector, industry, is_active — not a
time series. It has no business date, only `updated_at`, the insert time. Because
QuestDB requires the designated timestamp in DEDUP UPSERT KEYS, and the insert time
differs on every write, the table can never be deduplicated by the database.

insert_or_update_metadata() said as much in a comment ("QuestDB doesn't support
UPSERT easily — We'll just insert new records") and appended a fresh row per symbol
on every price and action collection run: 192,598 rows describing 953 symbols (202x).

The fix has two halves. This script compacts what is already stored; the write path
in db/questdb_client.py is changed to update in place, mirroring the pattern already
proven on eodhd_stock_metadata (now 1.0x).

Rows kept are last() per symbol — the most recent write, i.e. current state.

SAFETY
------
The old table is renamed, not dropped, until --drop-old is passed. Run a verified
backup first.
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

LIVE = 'eodhd_metadata'
NEW = 'eodhd_metadata_v2'
OLD = 'eodhd_metadata_old'

COLUMNS = [
    'symbol', 'exchange', 'name', 'sector', 'industry', 'currency',
    'last_price_update', 'last_fundamental_update', 'last_action_update',
    'last_calendar_update', 'has_fundamentals', 'has_dividends', 'has_splits',
    'total_price_records', 'total_fundamental_records', 'total_dividends',
    'total_splits', 'earliest_price_date', 'latest_price_date', 'last_error',
    'error_count', 'is_active', 'created_at', 'updated_at',
]

CREATE_NEW = f"""
CREATE TABLE {NEW} (
    symbol SYMBOL,
    exchange SYMBOL,
    name STRING,
    sector SYMBOL,
    industry SYMBOL,
    currency SYMBOL,
    last_price_update TIMESTAMP,
    last_fundamental_update TIMESTAMP,
    last_action_update TIMESTAMP,
    last_calendar_update TIMESTAMP,
    has_fundamentals BOOLEAN,
    has_dividends BOOLEAN,
    has_splits BOOLEAN,
    total_price_records LONG,
    total_fundamental_records INT,
    total_dividends INT,
    total_splits INT,
    earliest_price_date TIMESTAMP,
    latest_price_date TIMESTAMP,
    last_error STRING,
    error_count INT,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) timestamp(updated_at) PARTITION BY YEAR WAL
"""

EXTRACT = f"SELECT {', '.join(COLUMNS)} FROM {LIVE} ORDER BY updated_at"

# Descriptive fields are written as '' whenever the caller does not supply them, and
# most callers do not: of 192,598 rows only 945 carry a name and none carry a sector.
# Taking the newest row wholesale would therefore discard the single populated row
# each symbol has. For these fields we keep the most recent NON-EMPTY value instead;
# everything else takes the newest value as-is, because it is genuine current state.
STICKY = ('exchange', 'name', 'sector', 'industry', 'currency')


def _is_empty(value):
    return value is None or (isinstance(value, str) and not value.strip())


def collapse(rows):
    """One record per symbol: newest state, with sticky fields backfilled."""
    idx = {c: i for i, c in enumerate(COLUMNS)}
    latest = {}
    for row in rows:                      # already ordered oldest -> newest
        symbol = row[idx['symbol']]
        current = latest.get(symbol)
        if current is None:
            latest[symbol] = list(row)
            continue
        preserved = {c: current[idx[c]] for c in STICKY
                     if not _is_empty(current[idx[c]])}
        current[:] = list(row)
        for col, value in preserved.items():
            if _is_empty(current[idx[col]]):
                current[idx[col]] = value
    return [tuple(r) for r in latest.values()]


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
    cur.execute(f"SELECT count(*) FROM (SELECT symbol FROM {LIVE} GROUP BY symbol)")
    expected = cur.fetchone()[0]
    logger.info(f"Source  : {before:,} rows / {expected:,} symbols "
                f"({before/max(expected,1):.0f}x)")

    cur.execute(EXTRACT)
    raw = cur.fetchall()
    rows = collapse(raw)
    logger.info(f"Extracted: {len(rows):,} rows from {len(raw):,} "
                f"(newest state, sticky fields backfilled)")
    if len(rows) != expected:
        raise RuntimeError(f"Extraction returned {len(rows):,}, expected {expected:,}")

    named = sum(1 for r in rows if not _is_empty(r[COLUMNS.index('name')]))
    logger.info(f"  symbols with a name preserved: {named:,}")

    if dry_run:
        logger.info("DRY RUN — nothing written. Sample:")
        for r in rows[:5]:
            logger.info(f"    {r[0]:10s} name={str(r[2])[:32]!r}")
        return

    if table_exists(cur, NEW):
        cur.execute(f"DROP TABLE {NEW}")
    cur.execute(CREATE_NEW)
    time.sleep(1)

    # updated_at is the designated timestamp, so rows must arrive in its order.
    ts_index = COLUMNS.index('updated_at')
    rows = sorted(rows, key=lambda r: (r[ts_index] is None, r[ts_index]))

    sql = f"INSERT INTO {NEW} ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))})"
    execute_batch(cur, sql, rows, page_size=BATCH_INSERT_SIZE)
    time.sleep(3)

    cur.execute(f"SELECT count() FROM {NEW}")
    written = cur.fetchone()[0]
    logger.info(f"Written : {written:,} rows")
    if written != expected:
        raise RuntimeError(f"{NEW} holds {written:,}, expected {expected:,} — NOT swapping")

    cur.execute(f"SELECT count(*) FROM (SELECT symbol FROM {NEW} GROUP BY symbol)")
    logger.info(f"  distinct symbols : {cur.fetchone()[0]:,}")
    cur.execute(f"SELECT count() FROM {NEW} WHERE sector IS NOT NULL AND sector != ''")
    logger.info(f"  with sector      : {cur.fetchone()[0]:,}")

    if table_exists(cur, OLD):
        raise RuntimeError(f"{OLD} already exists — resolve it before swapping again")
    cur.execute(f"RENAME TABLE {LIVE} TO {OLD}")
    cur.execute(f"RENAME TABLE {NEW} TO {LIVE}")
    time.sleep(1)
    logger.info(f"Swapped : {LIVE} rebuilt, previous kept as {OLD}")
    logger.info(f"Old kept: re-run with --drop-old once satisfied")


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
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--drop-old', action='store_true')
    args = parser.parse_args()

    conn = connect()
    try:
        drop_old(conn) if args.drop_old else rebuild(conn, args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
