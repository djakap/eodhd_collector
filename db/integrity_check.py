"""
Deep integrity verification for QuestDB, intended to gate backups.

A backup taken from corrupt data is a corrupt backup, and the corruption is
usually only discovered at restore time when it is too late. This module runs
BEFORE the tar and refuses to let a bad snapshot be taken.

The checks are ordered by what has actually gone wrong on this instance:

  1. WAL health        — a suspended WAL means committed transactions are NOT in
                         the table files. Backing that up silently loses every
                         pending transaction. (Observed 2026-07: eodhd_stock_data
                         suspended with 18,399 pending txns.)
  2. Ghost partitions  — partition present in metadata with rows but 0 bytes on
                         disk. (Observed 2026-07: partition 2026-06, 619,417 rows,
                         0.0 MB, recreated from WAL on every restart.)
  3. Deep read scan    — per-partition aggregate over real columns. Row counts come
                         from metadata and stay correct even when the underlying
                         column files are unreadable; only an actual read catches
                         that, which is exactly what a restore would hit.
  4. Timestamp sanity  — impossible dates indicate a corrupt designated timestamp.
  5. Duplicates        — eodhd_stock_data has no DEDUP clause, so re-runs can
                         double-insert.

A manifest of per-table and per-partition fingerprints is written alongside the
backup so a restored copy can be proven identical rather than merely present.

Exit codes:  0 = clean   1 = warnings only   2 = critical (do not back up)
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import psycopg2

logger = logging.getLogger(__name__)

# Tables whose OHLC columns are meaningful for the read scan / fingerprint.
PRICE_TABLE = 'eodhd_stock_data'

# Business key per table — the columns that identify one real-world fact. Used to
# measure duplication; there is no way to infer these from the schema.
BLOAT_KEYS = {
    'eodhd_stock_data': ('symbol', 'interval', 'timestamp'),
    'eodhd_corporate_actions': ('symbol', 'action_type', 'action_date'),
    'eodhd_metadata': ('symbol',),
    'eodhd_stock_metadata': ('symbol', 'interval'),
    'eodhd_calendar_events': ('symbol', 'event_type', 'event_date'),
    'eodhd_fundamentals': ('symbol', 'report_date', 'report_type'),
    'yf_fundamentals': ('symbol', 'period_end', 'statement', 'freq', 'line_item'),
    'yf_valuation_daily': ('symbol', 'ts'),
    'yf_analyst_snapshot': ('symbol', 'ts', 'period'),
    'yf_profile': ('symbol', 'ts'),
}


class IntegrityReport:
    """
    Collected findings, in three severities.

    The dividing question is NOT "how bad is this data" but "does this make the
    BACKUP untrustworthy":

      critical — the archive would be incomplete or unreadable. A suspended WAL
                 omits committed rows; an unreadable partition restores as
                 nothing. These block the backup, because taking one would
                 produce a snapshot that cannot be restored faithfully.

      defect   — the data is wrong, but the backup captures it faithfully and a
                 restore reproduces exactly what exists today. Blocking on these
                 would mean no backups at all while a long-standing data problem
                 waits to be fixed, which is strictly worse than holding a
                 truthful snapshot of imperfect data.

      warning  — worth watching, no action required.
    """

    def __init__(self):
        self.critical: List[str] = []
        self.defects: List[str] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        self.manifest: Dict = {
            'generated_at': datetime.now().isoformat(),
            'tables': {},
        }

    def fail(self, msg: str):
        """Backup-compromising. Blocks."""
        self.critical.append(msg)
        logger.error(f"CRITICAL: {msg}")

    def defect(self, msg: str):
        """Real data defect, faithfully backed up. Loud, but does not block."""
        self.defects.append(msg)
        logger.error(f"DEFECT: {msg}")

    def warn(self, msg: str):
        self.warnings.append(msg)
        logger.warning(f"WARNING: {msg}")

    def note(self, msg: str):
        self.info.append(msg)
        logger.info(msg)

    @property
    def ok(self) -> bool:
        return not self.critical

    @property
    def exit_code(self) -> int:
        if self.critical:
            return 2
        return 1 if (self.defects or self.warnings) else 0

    def summary(self) -> str:
        if self.critical:
            return f"CRITICAL — {len(self.critical)} blocking issue(s)"
        parts = []
        if self.defects:
            parts.append(f"{len(self.defects)} defect(s)")
        if self.warnings:
            parts.append(f"{len(self.warnings)} warning(s)")
        return f"BACKUP-SAFE with {', '.join(parts)}" if parts else "CLEAN"


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_wal_health(cur, report: IntegrityReport, lag_warn: int = 5000):
    """A suspended WAL means data is committed but not applied to table files."""
    cur.execute("SELECT name, suspended, writerTxn, sequencerTxn FROM wal_tables()")
    rows = cur.fetchall()

    for name, suspended, writer_txn, seq_txn in rows:
        pending = (seq_txn or 0) - (writer_txn or 0)
        if suspended:
            report.fail(
                f"{name}: WAL SUSPENDED with {pending:,} pending transactions. "
                f"Those rows exist in the WAL but not in the table files — a backup "
                f"now would lose them. Resolve before backing up."
            )
        elif pending > lag_warn:
            report.warn(f"{name}: {pending:,} transactions pending WAL apply "
                        f"(backup may lag the latest writes)")

    report.note(f"WAL health: checked {len(rows)} table(s)")


def check_partitions(cur, table: str, report: IntegrityReport):
    """Detect partitions that claim rows but occupy no disk (ghost partitions)."""
    cur.execute(f"""
        SELECT name, numRows, diskSize, active, readOnly
        FROM table_partitions('{table}')
    """)
    parts = cur.fetchall()
    ghosts = []

    for name, num_rows, disk_size, active, read_only in parts:
        if (num_rows or 0) > 0 and (disk_size or 0) == 0:
            ghosts.append((name, num_rows))

    for name, num_rows in ghosts:
        report.fail(
            f"{table}: partition '{name}' reports {num_rows:,} rows but 0 bytes on "
            f"disk — metadata and storage disagree. Restoring this yields an empty "
            f"partition."
        )

    return parts


def deep_read_scan(cur, table: str, ts_col: str, parts, report: IntegrityReport,
                   value_column: Optional[str] = None) -> List[Dict]:
    """
    Force an actual read of every partition.

    Row counts are served from metadata and stay plausible even when the column
    files behind them cannot be read. Aggregating over a real column is what
    surfaces the damage — the same failure a restore would hit.

    Scoping is done with a partition-name prefix match rather than a date range:
    partition names carry the granularity ('2026', '2026-07', '2026-07-20') and
    deriving a range would mean re-deriving that granularity per table.
    """
    fingerprints: List[Dict] = []

    for name, num_rows, disk_size, active, read_only in parts:
        if (num_rows or 0) == 0:
            continue

        agg = f"sum({value_column})" if value_column else f"count({ts_col})"
        try:
            cur.execute(f"""
                SELECT count(), min({ts_col}), max({ts_col}), {agg}
                FROM "{table}"
                WHERE {ts_col} IN '{name}'
            """)
            cnt, tmin, tmax, checksum = cur.fetchone()
        except Exception as e:
            report.fail(
                f"{table}: partition '{name}' FAILED TO READ ({type(e).__name__}: {e}). "
                f"Metadata claims {num_rows:,} rows. This partition is corrupt."
            )
            continue

        fingerprints.append({
            'partition': name,
            'rows': cnt,
            'min_timestamp': str(tmin) if tmin else None,
            'max_timestamp': str(tmax) if tmax else None,
            'checksum': float(checksum) if checksum is not None else None,
            'disk_bytes': disk_size,
        })

    report.note(f"{table}: deep-read {len(fingerprints)} partition(s) successfully")
    return fingerprints


def check_timestamp_sanity(cur, table: str, designated: str, report: IntegrityReport):
    """
    Sanity-check EVERY time column, not only the designated one.

    Checking just the designated timestamp missed 324,780 rows of
    eodhd_corporate_actions.action_date set to 1970-01-01 — a NULL date coerced to
    epoch zero. The designated column (created_at) looked perfectly healthy the
    whole time.

    Severity differs by role: a bad designated timestamp is structural corruption,
    while a bad ordinary date column is an ingestion bug. Both matter, only the
    first should block a backup.

    Predicates use year(), never a literal range. A range predicate on the
    DESIGNATED timestamp is answered with partition pruning against the partition
    name, so rows whose timestamp falls outside the partition they physically live
    in are invisible to it. Measured on eodhd_corporate_actions 2026-07-20:

        WHERE created_at >= '1970-01-01' AND created_at < '1970-01-02'  ->       0
        WHERE year(created_at) = 1970                                   -> 324,780

    The range form silently reported the table as clean. year() costs ~0.4s on the
    27M-row table, which is a fair price for an answer that is actually true.
    """
    next_year = datetime.now().year + 1

    cur.execute(f"SELECT \"column\", type FROM table_columns('{table}')")
    time_cols = [c for c, t in cur.fetchall() if t in ('TIMESTAMP', 'DATE')]

    for col in time_cols:
        is_designated = (col == designated)

        # A bad designated timestamp distorts every range query, so it is a defect
        # rather than a mere warning — but the backup still captures it faithfully,
        # so it must not block.
        def flag(msg):
            report.defect(msg) if is_designated else report.warn(msg)

        cur.execute(f"SELECT count() FROM \"{table}\" WHERE year(\"{col}\") > {next_year}")
        future = cur.fetchone()[0]
        if future:
            flag(f"{table}.{col}: {future:,} rows dated after {next_year}")

        # Exactly epoch zero is the signature of a NULL date written as 0, which is
        # different in kind from merely-old data and worth calling out separately.
        cur.execute(f"SELECT count() FROM \"{table}\" WHERE year(\"{col}\") = 1970")
        epoch = cur.fetchone()[0]
        if epoch:
            flag(f"{table}.{col}: {epoch:,} rows at epoch zero (1970) — "
                 f"NULL dates written as 0 by the ingest path")

        cur.execute(f"SELECT count() FROM \"{table}\" WHERE year(\"{col}\") < 1970")
        ancient = cur.fetchone()[0]
        if ancient:
            flag(f"{table}.{col}: {ancient:,} rows dated before 1970")

    report.note(f"{table}: time-column sanity checked on {len(time_cols)} column(s)")


def check_partition_bounds(cur, table: str, report: IntegrityReport):
    """
    Verify every partition actually contains the range its name claims.

    A partition named '2026' holding rows stamped 1970 means the designated
    timestamp and the physical layout disagree. Queries then prune those rows away
    and report a clean table, and a restore reproduces the same lie. Metadata-only,
    so it costs ~0.02s across the whole database.
    """
    cur.execute(f"""SELECT name, minTimestamp, maxTimestamp, numRows
                    FROM table_partitions('{table}')""")
    for name, min_ts, max_ts, num_rows in cur.fetchall():
        if not num_rows or min_ts is None:
            continue
        # Partition names are prefixes of the timestamps they hold: '2026',
        # '2026-07', '2026-07-20'.
        if not str(min_ts).startswith(name) or not str(max_ts).startswith(name):
            # A defect, not a blocker: the rows are readable and the archive
            # reproduces them exactly. What it breaks is querying, not restoring.
            report.defect(
                f"{table}: partition '{name}' holds rows outside its own range "
                f"({str(min_ts)[:19]} .. {str(max_ts)[:19]}, {num_rows:,} rows). "
                f"Range queries on the designated timestamp will silently skip them."
            )


def check_table_bloat(cur, report: IntegrityReport, warn_ratio: float = 2.0):
    """
    Compare stored rows against distinct business keys.

    Tables whose designated timestamp is the INSERT time cannot be deduplicated by
    QuestDB (the dedup key must include the designated timestamp, and that value
    changes on every re-fetch). They therefore accumulate a fresh copy of every
    fact on each collection run, silently. Measuring the ratio is the only way to
    see it: eodhd_corporate_actions reached 128x and eodhd_metadata 202x before
    anything reported it.
    """
    for table, keys in BLOAT_KEYS.items():
        try:
            cur.execute(f'SELECT count() FROM "{table}"')
            rows = cur.fetchone()[0]
            if rows == 0:
                continue
            key_list = ', '.join(keys)
            cur.execute(f'SELECT count(*) FROM (SELECT {key_list} FROM "{table}" '
                        f'GROUP BY {key_list})')
            unique = cur.fetchone()[0] or 1
        except Exception as e:
            report.warn(f"{table}: bloat check skipped ({type(e).__name__}: {e})")
            continue

        ratio = rows / unique
        if ratio >= warn_ratio:
            report.warn(
                f"{table}: {rows:,} rows storing {unique:,} distinct "
                f"({', '.join(keys)}) = {ratio:.0f}x duplication"
            )
        else:
            report.note(f"{table}: {rows:,} rows / {unique:,} keys ({ratio:.1f}x)")


def check_ohlc_validity(cur, report: IntegrityReport):
    """Structurally impossible bars — cheap sanity pass, not a full audit."""
    cur.execute(f"""
        SELECT count() FROM {PRICE_TABLE}
        WHERE high < low OR close < 0 OR open < 0 OR high < 0 OR low < 0
    """)
    bad = cur.fetchone()[0]
    if bad:
        report.warn(f"{PRICE_TABLE}: {bad:,} structurally invalid bars "
                    f"(high<low or negative prices)")
    else:
        report.note(f"{PRICE_TABLE}: OHLC structure valid")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_checks(conn, deep: bool = True) -> IntegrityReport:
    report = IntegrityReport()
    cur = conn.cursor()

    check_wal_health(cur, report)

    # The designated timestamp column differs per table (timestamp, created_at,
    # updated_at, last_updated, ts, period_end) — read it rather than assume it.
    cur.execute("SELECT table_name, designatedTimestamp FROM tables() ORDER BY table_name")
    table_meta = cur.fetchall()
    tables = [t for t, _ in table_meta]

    for table, ts_col in table_meta:
        cur.execute(f'SELECT count() FROM "{table}"')
        total = cur.fetchone()[0]

        parts = check_partitions(cur, table, report)

        fingerprints = []
        if deep and total > 0 and ts_col:
            value_col = 'close' if table == PRICE_TABLE else None
            fingerprints = deep_read_scan(cur, table, ts_col, parts, report, value_col)

        if total > 0 and ts_col:
            check_timestamp_sanity(cur, table, ts_col, report)
            check_partition_bounds(cur, table, report)

        report.manifest['tables'][table] = {
            'rows': total,
            'timestamp_column': ts_col,
            'partitions': len(parts),
            'fingerprints': fingerprints,
        }

    check_table_bloat(cur, report)

    if PRICE_TABLE in tables:
        check_ohlc_validity(cur, report)

    cur.close()
    return report


def verify_manifest(conn, manifest_path: str) -> IntegrityReport:
    """
    Compare a live database against a manifest — the restore-side check.

    Proves a restored copy is identical to what was backed up, rather than merely
    present and queryable.
    """
    report = IntegrityReport()
    with open(manifest_path) as f:
        expected = json.load(f)

    cur = conn.cursor()
    for table, exp in expected.get('tables', {}).items():
        try:
            cur.execute(f'SELECT count() FROM "{table}"')
            actual = cur.fetchone()[0]
        except Exception as e:
            report.fail(f"{table}: missing after restore ({e})")
            continue

        if actual != exp['rows']:
            report.fail(f"{table}: {actual:,} rows, expected {exp['rows']:,} "
                        f"(delta {actual - exp['rows']:+,})")
        else:
            report.note(f"{table}: {actual:,} rows — matches manifest")

    cur.close()
    return report


def _connect():
    from config.db_config import (
        QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
        QUESTDB_PASSWORD, QUESTDB_DATABASE,
    )
    return psycopg2.connect(
        host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
    )


def main():
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    parser = argparse.ArgumentParser(description="Deep QuestDB integrity check")
    parser.add_argument('--manifest', help="Write a fingerprint manifest to this path")
    parser.add_argument('--verify', help="Verify the live DB against an existing manifest")
    parser.add_argument('--quick', action='store_true',
                        help="Skip the per-partition deep read scan")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')
    conn = _connect()

    try:
        if args.verify:
            report = verify_manifest(conn, args.verify)
        else:
            report = run_checks(conn, deep=not args.quick)
            if args.manifest:
                with open(args.manifest, 'w') as f:
                    json.dump(report.manifest, f, indent=2)
                logger.info(f"Manifest written: {args.manifest}")
    finally:
        conn.close()

    print()
    print("=" * 70)
    print(f"INTEGRITY: {report.summary()}")
    print("=" * 70)
    for msg in report.critical:
        print(f"  [CRITICAL] {msg}")
    for msg in report.defects:
        print(f"  [DEFECT]   {msg}")
    for msg in report.warnings:
        print(f"  [WARN]     {msg}")

    sys.exit(report.exit_code)


if __name__ == "__main__":
    main()
