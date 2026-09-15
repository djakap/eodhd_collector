#!/usr/bin/env python3
"""Restore production corporate actions from the verified 2026-09-10 archive.

The declared supported range of this restore is 2000-06-27 through 2026-09-10,
inclusive: 4,523 source rows across 27 yearly partitions. The source must be a
separately opened, verified copy of that archive. Running without a mode is a
dry run. Building and swapping are separate deliberate commands so the new
table is fully verified before the damaged table is renamed.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime
import os
import re
import sys
import time
from typing import Optional, Sequence

import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import (
    BATCH_INSERT_SIZE,
    QUESTDB_DATABASE,
    QUESTDB_HOST,
    QUESTDB_PASSWORD,
    QUESTDB_PG_PORT,
    QUESTDB_USER,
)
from config.tables import TABLE_ACTIONS_PRODUCTION


MODE_DRY_RUN = "dry-run"
MODE_BUILD = "build"
MODE_SWAP = "swap"

SOURCE_TABLE = TABLE_ACTIONS_PRODUCTION
BUILD_TABLE = "corporate_actions_qcf013_restore"
DAMAGED_TABLE = "corporate_actions_damaged_20260912"

EXPECTED_ROWS = 4523
EXPECTED_PARTITIONS = 27
EXPECTED_MIN = datetime(2000, 6, 27)
EXPECTED_MAX = datetime(2026, 9, 10)

COLUMNS = (
    "symbol",
    "action_type",
    "action_date",
    "dividend_amount",
    "dividend_currency",
    "payment_date",
    "record_date",
    "declaration_date",
    "dividend_type",
    "split_ratio",
    "split_from",
    "split_to",
    "created_at",
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TableStats:
    rows: int
    minimum: datetime
    maximum: datetime
    partitions: int
    partition_rows: int
    full_projection_rows: int


def _quoted_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe QuestDB table identifier: {value!r}")
    return f'"{value}"'


def table_exists(cursor, table: str) -> bool:
    cursor.execute("SELECT count(*) FROM tables() WHERE table_name = %s", (table,))
    return int(cursor.fetchone()[0]) > 0


def table_stats(cursor, table: str) -> TableStats:
    identifier = _quoted_identifier(table)
    projection = ", ".join(COLUMNS)
    cursor.execute(f"SELECT count(), min(action_date), max(action_date) FROM {identifier}")
    rows, minimum, maximum = cursor.fetchone()
    cursor.execute(
        "SELECT count(), sum(numRows) FROM table_partitions(%s)",
        (table,),
    )
    partitions, partition_rows = cursor.fetchone()
    cursor.execute(
        f"SELECT count() FROM (SELECT {projection} FROM {identifier})"
    )
    full_projection_rows = cursor.fetchone()[0]
    return TableStats(
        rows=int(rows),
        minimum=minimum,
        maximum=maximum,
        partitions=int(partitions),
        partition_rows=int(partition_rows or 0),
        full_projection_rows=int(full_projection_rows),
    )


def validate_archive_stats(stats: TableStats) -> None:
    expected = TableStats(
        EXPECTED_ROWS,
        EXPECTED_MIN,
        EXPECTED_MAX,
        EXPECTED_PARTITIONS,
        EXPECTED_ROWS,
        EXPECTED_ROWS,
    )
    if stats != expected:
        raise RuntimeError(f"Archive source does not match its verified manifest: {stats!r}")


def validate_empty_build_target(exists: bool, row_count: Optional[int]) -> None:
    if exists and row_count:
        raise RuntimeError(
            f"Refusing non-empty build target {BUILD_TABLE!r}: {row_count} row(s)"
        )


def _connect(host: str, port: int):
    connection = psycopg2.connect(
        host=host,
        port=port,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DATABASE,
    )
    connection.autocommit = True
    return connection


def _target_state(cursor) -> tuple[bool, Optional[int]]:
    exists = table_exists(cursor, BUILD_TABLE)
    if not exists:
        return False, None
    cursor.execute(f"SELECT count() FROM {_quoted_identifier(BUILD_TABLE)}")
    return True, int(cursor.fetchone()[0])


def _create_build_table(cursor) -> None:
    cursor.execute(
        f"""
        CREATE TABLE {_quoted_identifier(BUILD_TABLE)} (
            symbol SYMBOL,
            action_type SYMBOL,
            action_date TIMESTAMP,
            dividend_amount DOUBLE,
            dividend_currency SYMBOL,
            payment_date TIMESTAMP,
            record_date TIMESTAMP,
            declaration_date TIMESTAMP,
            dividend_type SYMBOL,
            split_ratio STRING,
            split_from INT,
            split_to INT,
            created_at TIMESTAMP
        ) TIMESTAMP(action_date) PARTITION BY YEAR WAL
          DEDUP UPSERT KEYS(action_date, symbol, action_type)
        """
    )


def _wait_for_verified_target(cursor, timeout_seconds: int = 60) -> TableStats:
    deadline = time.monotonic() + timeout_seconds
    latest = None
    while time.monotonic() < deadline:
        latest = table_stats(cursor, BUILD_TABLE)
        if latest.rows == EXPECTED_ROWS and latest.partition_rows == EXPECTED_ROWS:
            validate_archive_stats(latest)
            return latest
        time.sleep(1)
    raise RuntimeError(f"Build target did not settle to the archive manifest: {latest!r}")


def dry_run(source, target) -> None:
    source_stats = table_stats(source.cursor(), SOURCE_TABLE)
    validate_archive_stats(source_stats)
    target_cursor = target.cursor()
    exists, row_count = _target_state(target_cursor)
    validate_empty_build_target(exists, row_count)
    print(f"DRY RUN source verified: {source_stats!r}")
    print(f"DRY RUN target: exists={exists}, rows={row_count}; no write statements issued")


def build(source, target) -> None:
    source_cursor = source.cursor()
    source_stats = table_stats(source_cursor, SOURCE_TABLE)
    validate_archive_stats(source_stats)

    target_cursor = target.cursor()
    exists, row_count = _target_state(target_cursor)
    validate_empty_build_target(exists, row_count)
    if not exists:
        _create_build_table(target_cursor)
        print(f"Created empty build table {BUILD_TABLE}")

    source_cursor.execute(
        f"SELECT {', '.join(COLUMNS)} FROM {_quoted_identifier(SOURCE_TABLE)} "
        "ORDER BY action_date, symbol, action_type"
    )
    insert_sql = (
        f"INSERT INTO {_quoted_identifier(BUILD_TABLE)} ({', '.join(COLUMNS)}) "
        f"VALUES ({', '.join(['%s'] * len(COLUMNS))})"
    )
    copied = 0
    while True:
        rows = source_cursor.fetchmany(BATCH_INSERT_SIZE)
        if not rows:
            break
        execute_batch(target_cursor, insert_sql, rows, page_size=BATCH_INSERT_SIZE)
        copied += len(rows)
        print(f"Copied {copied}/{EXPECTED_ROWS} archive row(s)")

    if copied != EXPECTED_ROWS:
        raise RuntimeError(f"Copied {copied} rows, expected {EXPECTED_ROWS}")
    stats = _wait_for_verified_target(target_cursor)
    print(f"Build verified before swap: {stats!r}")


def swap(target) -> None:
    cursor = target.cursor()
    if not table_exists(cursor, BUILD_TABLE):
        raise RuntimeError(f"Verified build table {BUILD_TABLE!r} does not exist")
    validate_archive_stats(table_stats(cursor, BUILD_TABLE))
    if not table_exists(cursor, SOURCE_TABLE):
        raise RuntimeError(f"Damaged live table {SOURCE_TABLE!r} does not exist")
    if table_exists(cursor, DAMAGED_TABLE):
        raise RuntimeError(f"Evidence table {DAMAGED_TABLE!r} already exists")

    first = f"RENAME TABLE {_quoted_identifier(SOURCE_TABLE)} TO {_quoted_identifier(DAMAGED_TABLE)}"
    second = f"RENAME TABLE {_quoted_identifier(BUILD_TABLE)} TO {_quoted_identifier(SOURCE_TABLE)}"
    cursor.execute(first)
    print(first)
    try:
        cursor.execute(second)
        print(second)
    except Exception:
        rollback = (
            f"RENAME TABLE {_quoted_identifier(DAMAGED_TABLE)} "
            f"TO {_quoted_identifier(SOURCE_TABLE)}"
        )
        cursor.execute(rollback)
        print(f"Second rename failed; rolled back first rename with: {rollback}")
        raise

    stats = table_stats(cursor, SOURCE_TABLE)
    validate_archive_stats(stats)
    print(f"Swap verified: {SOURCE_TABLE}={stats!r}; damaged table retained as {DAMAGED_TABLE}")


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--build", dest="mode", action="store_const", const=MODE_BUILD)
    modes.add_argument("--swap", dest="mode", action="store_const", const=MODE_SWAP)
    parser.set_defaults(mode=MODE_DRY_RUN)
    parser.add_argument("--source-host", default="127.0.0.1")
    parser.add_argument("--source-port", type=int, required=True)
    parser.add_argument("--target-host", default=QUESTDB_HOST)
    parser.add_argument("--target-port", type=int, default=QUESTDB_PG_PORT)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    source = _connect(args.source_host, args.source_port)
    target = _connect(args.target_host, args.target_port)
    try:
        if args.mode == MODE_DRY_RUN:
            dry_run(source, target)
        elif args.mode == MODE_BUILD:
            build(source, target)
        else:
            swap(target)
    finally:
        source.close()
        target.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
