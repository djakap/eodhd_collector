#!/usr/bin/env python3
"""Backfill truthful production price coverage into ``stock_metadata``.

Run the dry mode first, inspect its pair count/sample, then opt in to the write:

    python scripts/backfill_stock_metadata.py --dry-run
    python scripts/backfill_stock_metadata.py --apply

The script reads only ``stock_data`` and writes only ``stock_metadata``. It
refuses to append to a non-empty target so an accidental rerun cannot create a
second daily row for every pair.
"""

import argparse
from datetime import datetime
from pathlib import Path
import sys
import time

import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.db_config import (
    BATCH_INSERT_SIZE,
    QUESTDB_DATABASE,
    QUESTDB_HOST,
    QUESTDB_PASSWORD,
    QUESTDB_PG_PORT,
    QUESTDB_USER,
)
from config.tables import (
    TABLE_PRICE_METADATA_PRODUCTION,
    TABLE_PRICES_PRODUCTION,
)


COVERAGE_SQL = f"""
    SELECT symbol, interval, count(), min(timestamp), max(timestamp)
    FROM {TABLE_PRICES_PRODUCTION}
    GROUP BY symbol, interval
    ORDER BY symbol, interval
"""

INSERT_SQL = f"""
    INSERT INTO {TABLE_PRICE_METADATA_PRODUCTION}
        (symbol, interval, last_updated, total_records,
         data_start, data_end, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def connect():
    connection = psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PG_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DATABASE,
    )
    connection.autocommit = True
    return connection


def _target_counts(cursor):
    cursor.execute(f"SELECT count() FROM {TABLE_PRICE_METADATA_PRODUCTION}")
    row_count = cursor.fetchone()[0]
    cursor.execute(
        f"SELECT count() FROM (SELECT symbol, interval "
        f"FROM {TABLE_PRICE_METADATA_PRODUCTION} GROUP BY symbol, interval)"
    )
    pair_count = cursor.fetchone()[0]
    return row_count, pair_count


def backfill(connection, dry_run: bool) -> int:
    cursor = connection.cursor()
    try:
        cursor.execute(COVERAGE_SQL)
        coverage = cursor.fetchall()
        expected = len(coverage)
        print(f"production_pairs={expected}")
        for symbol, interval, count, data_start, data_end in coverage[:5]:
            print(
                f"sample={symbol}/{interval} rows={count} "
                f"range={data_start}..{data_end}"
            )

        if expected == 0:
            print("ERROR: production coverage query returned no pairs", file=sys.stderr)
            return 1

        before_rows, before_pairs = _target_counts(cursor)
        print(f"target_before_rows={before_rows} target_before_pairs={before_pairs}")

        if dry_run:
            print("DRY RUN: no rows written")
            return 0

        if before_rows or before_pairs:
            print(
                "ERROR: stock_metadata is not empty; refusing to append",
                file=sys.stderr,
            )
            return 1

        written_at = datetime.now()
        rows = [
            (symbol, interval, written_at, count, data_start, data_end, written_at)
            for symbol, interval, count, data_start, data_end in coverage
        ]
        execute_batch(cursor, INSERT_SQL, rows, page_size=BATCH_INSERT_SIZE)

        # WAL application is asynchronous. Poll only the new table until the
        # exact row/pair counts are visible or the bounded verification expires.
        deadline = time.monotonic() + 30
        while True:
            after_rows, after_pairs = _target_counts(cursor)
            if after_rows == expected and after_pairs == expected:
                break
            if time.monotonic() >= deadline:
                print(
                    f"ERROR: wrote {after_rows} rows/{after_pairs} pairs, "
                    f"expected {expected}",
                    file=sys.stderr,
                )
                return 1
            time.sleep(0.5)

        print(f"written_rows={after_rows} written_pairs={after_pairs}")
        return 0
    finally:
        cursor.close()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="read and report only")
    mode.add_argument("--apply", action="store_true", help="write the empty target table")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    connection = connect()
    try:
        return backfill(connection, dry_run=args.dry_run)
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
