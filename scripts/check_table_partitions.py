#!/usr/bin/env python3
"""Report QuestDB partition metadata/readability divergence without repairing it."""

from dataclasses import dataclass
import os
import re
import sys
from typing import Iterable, Optional, Sequence, Tuple

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import (
    QUESTDB_DATABASE,
    QUESTDB_HOST,
    QUESTDB_PASSWORD,
    QUESTDB_PG_PORT,
    QUESTDB_USER,
)
from config.tables import TABLE_ACTIONS_PRODUCTION


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class PartitionState:
    table: str
    partition_count: int
    partition_rows: int
    readable_rows: Optional[int]
    problem: bool
    deferred: bool
    message: str


def _quoted_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe QuestDB table identifier: {value!r}")
    return f'"{value}"'


def assess_partition_state(
    table: str,
    partitions: Sequence[Tuple[str, int]],
    readable_rows: Optional[int],
    suspended: bool,
    readability_error: Optional[str] = None,
) -> PartitionState:
    """Assess already-measured state; kept pure for database-free tests."""
    partition_count = len(partitions)
    partition_rows = sum(row_count for _, row_count in partitions)

    if suspended:
        message = f"{table}: deferred to existing WAL heal path (table is suspended)"
        return PartitionState(
            table,
            partition_count,
            partition_rows,
            readable_rows,
            problem=False,
            deferred=True,
            message=message,
        )

    if readability_error is not None:
        message = (
            f"{table}: PARTITION READABILITY FAILURE; metadata has "
            f"{partition_rows} rows across {partition_count} partitions; "
            f"read failed: {readability_error}"
        )
        return PartitionState(
            table,
            partition_count,
            partition_rows,
            readable_rows,
            problem=True,
            deferred=False,
            message=message,
        )

    if readable_rows != partition_rows:
        message = (
            f"{table}: PARTITION DIVERGENCE; metadata has {partition_rows} rows "
            f"across {partition_count} partitions but SELECT count() reads "
            f"{readable_rows}"
        )
        return PartitionState(
            table,
            partition_count,
            partition_rows,
            readable_rows,
            problem=True,
            deferred=False,
            message=message,
        )

    message = f"{table}: healthy ({readable_rows} rows across {partition_count} partitions)"
    return PartitionState(
        table,
        partition_count,
        partition_rows,
        readable_rows,
        problem=False,
        deferred=False,
        message=message,
    )


def inspect_table(connection, table: str) -> PartitionState:
    """Read one table's WAL, partition and count state using SELECT statements only."""
    identifier = _quoted_identifier(table)
    cursor = connection.cursor()
    cursor.execute(
        "SELECT suspended FROM wal_tables() WHERE name = %s",
        (table,),
    )
    wal_row = cursor.fetchone()
    suspended = bool(wal_row and wal_row[0])
    if suspended:
        return assess_partition_state(table, [], None, suspended=True)

    cursor.execute(
        "SELECT name, numRows FROM table_partitions(%s) ORDER BY name",
        (table,),
    )
    partitions = [(str(name), int(rows)) for name, rows in cursor.fetchall()]

    try:
        cursor.execute(f"SELECT count() FROM {identifier}")
        readable_rows = int(cursor.fetchone()[0])
    except Exception as exc:  # report the database's exact readability failure
        connection.rollback()
        return assess_partition_state(
            table,
            partitions,
            readable_rows=None,
            suspended=False,
            readability_error=f"{type(exc).__name__}: {exc}",
        )

    return assess_partition_state(
        table,
        partitions,
        readable_rows=readable_rows,
        suspended=False,
    )


def inspect_tables(
    tables: Iterable[str] = (TABLE_ACTIONS_PRODUCTION,),
    connection=None,
) -> list[PartitionState]:
    """Inspect tables and return findings; this function never changes the database."""
    owns_connection = connection is None
    if owns_connection:
        connection = psycopg2.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PG_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database=QUESTDB_DATABASE,
        )
    try:
        return [inspect_table(connection, table) for table in tables]
    finally:
        if owns_connection:
            connection.close()


def main() -> int:
    findings = inspect_tables()
    for finding in findings:
        print(finding.message)
    return 1 if any(finding.problem for finding in findings) else 0


if __name__ == "__main__":
    raise SystemExit(main())
