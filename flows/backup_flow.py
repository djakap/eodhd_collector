"""
Prefect Flow: QuestDB Filesystem Backup

Directly tars the QuestDB data volume to a .tar.gz file.
Safe for live databases — historical partitions are immutable in QuestDB.

The backup is gated and verified, because a backup is only worth what it can
restore:

  1. PRE   — deep integrity check. A snapshot taken while a WAL is suspended
             silently omits every pending transaction, and a ghost partition
             restores as empty. Both have happened on this instance, so a
             critical finding aborts the backup instead of preserving damage.
  2. TAR   — archive the volume.
  3. POST  — read the whole archive back. `size > 0` does not detect a truncated
             or CRC-broken gzip; streaming every member does.
  4. MANIFEST — per-table and per-partition fingerprints written next to the
             archive, so a restored copy can be proven identical rather than
             merely present.

Restore on another machine:
  tar -xzf questdb_backup_2026-05-28_160000.tar.gz
  docker run -p 9001:9000 -p 8812:8812 \\
    -v $(pwd)/questdb-data:/var/lib/questdb \\
    questdb/questdb:7.3.10

Then prove the restore is complete:
  python db/integrity_check.py --verify questdb_backup_2026-05-28_160000.manifest.json
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Dict

from prefect import flow, task, get_run_logger

import psycopg2
from db.integrity_check import run_checks
from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)

QUESTDB_DATA_PATH = Path("/questdb-data")
BACKUP_DIR = Path("/backup")


@task(name="verify-integrity", retries=0)
def verify_integrity(deep: bool = True) -> Dict:
    """
    Deep integrity check. Raises on any critical finding so the backup aborts.

    Backing up a damaged database produces a damaged backup that looks healthy
    until the day it is needed.
    """
    logger = get_run_logger()
    conn = psycopg2.connect(
        host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
    )
    try:
        report = run_checks(conn, deep=deep)
    finally:
        conn.close()

    # Defects are real data problems, but the archive captures them faithfully —
    # a restore reproduces exactly today's state. Blocking on them would leave the
    # database with no backups at all while the defect waits to be fixed.
    for msg in report.defects:
        logger.error(f"  DEFECT: {msg}")
    for msg in report.warnings:
        logger.warning(f"  {msg}")

    if not report.ok:
        for msg in report.critical:
            logger.error(f"  {msg}")
        raise RuntimeError(
            f"Integrity check FAILED with {len(report.critical)} critical issue(s) — "
            f"backup aborted. These mean the archive would be incomplete or "
            f"unreadable, not merely imperfect. Fix the database first."
        )

    total_rows = sum(t['rows'] for t in report.manifest['tables'].values())
    logger.info(f"Integrity: {report.summary()} — "
                f"{len(report.manifest['tables'])} tables, {total_rows:,} rows verified")
    return report.manifest


def _snapshot_conn():
    conn = psycopg2.connect(
        host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
        password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
    )
    conn.autocommit = True
    return conn


@task(name="snapshot-prepare")
def snapshot_prepare():
    """
    Hold a consistent, immutable view of the data files for the duration of the
    tar. Without this the backup tars a LIVE database: QuestDB rewrites partition
    files (WAL apply, merges) and the intraday flow writes while the archive is
    being built, so files change mid-read and the archive fails verification
    ("30,612 files readable but 30,624 were archived"). SNAPSHOT PREPARE pins the
    files being copied so writes continue into new files instead.
    """
    logger = get_run_logger()
    conn = _snapshot_conn()
    cur = conn.cursor()
    try:
        # Release any snapshot left dangling by a previously crashed backup
        # (e.g. the laptop slept mid-run) — otherwise PREPARE errors.
        try:
            cur.execute("SNAPSHOT COMPLETE")
        except Exception:
            pass
        cur.execute("SNAPSHOT PREPARE")
        logger.info("SNAPSHOT PREPARE — data files pinned for a consistent copy")
    finally:
        conn.close()


@task(name="snapshot-complete")
def snapshot_complete():
    """Release the snapshot so QuestDB resumes normal housekeeping. Always run,
    even when the tar failed, so a snapshot is never left held."""
    logger = get_run_logger()
    conn = _snapshot_conn()
    cur = conn.cursor()
    try:
        cur.execute("SNAPSHOT COMPLETE")
        logger.info("SNAPSHOT COMPLETE — snapshot released")
    finally:
        conn.close()


@task(name="tar-questdb-data")
def create_backup(backup_name: str) -> Dict:
    logger = get_run_logger()
    archive_path = BACKUP_DIR / f"{backup_name}.tar.gz"

    # Entries are added one at a time rather than with a single recursive tar.add.
    # QuestDB rewrites partition directories on merge and removes the superseded
    # copies, and DROP TABLE deletes a directory outright — so paths legitimately
    # vanish between listing and archiving. A recursive add aborts the entire backup
    # on the first such disappearance (observed: FileNotFoundError on a table
    # directory removed mid-run), which is the wrong outcome: those files are not
    # part of the consistent state we are capturing, and skipping them is correct.
    entries = []
    total_bytes = 0
    for path in sorted(QUESTDB_DATA_PATH.rglob("*")):
        try:
            if path.is_file():
                total_bytes += path.stat().st_size
            entries.append(path)
        except (FileNotFoundError, OSError):
            continue

    logger.info(f"Archiving {total_bytes/1e9:.2f} GB ({len(entries):,} entries) "
                f"→ {archive_path.name}")

    done_bytes = 0
    last_pct = 0
    archived_files = 0
    vanished = 0

    try:
        with tarfile.open(archive_path, "w:gz") as tar:
            for path in entries:
                arcname = Path("questdb-data") / path.relative_to(QUESTDB_DATA_PATH)
                try:
                    is_file = path.is_file()
                    size = path.stat().st_size if is_file else 0
                    tar.add(path, arcname=str(arcname), recursive=False)
                except (FileNotFoundError, OSError):
                    vanished += 1
                    continue

                if is_file:
                    archived_files += 1
                    done_bytes += size
                    pct = int(done_bytes / total_bytes * 100) if total_bytes else 100
                    if pct >= last_pct + 10:
                        last_pct = pct - (pct % 10)
                        logger.info(f"  {last_pct}% — {done_bytes/1e9:.2f} / "
                                    f"{total_bytes/1e9:.2f} GB")
    except BaseException:
        # A half-written archive still opens and reads cleanly up to the truncation
        # point, so it is indistinguishable from a good one by eye and would occupy a
        # retention slot. Leaving it behind is worse than having no archive at all.
        if archive_path.exists():
            archive_path.unlink()
            logger.error(f"Backup failed — removed partial archive {archive_path.name}")
        raise

    if vanished:
        # Normal in small numbers (housekeeping); a large share means the database is
        # being rewritten wholesale underneath us and the snapshot is not coherent.
        share = vanished / max(len(entries), 1)
        msg = f"{vanished:,} entr(ies) disappeared during archiving (QuestDB housekeeping)"
        if share > 0.05:
            raise RuntimeError(f"{msg} — {share:.0%} of the tree, snapshot not coherent")
        logger.warning(f"  {msg}")

    compressed_gb = archive_path.stat().st_size / 1e9
    logger.info(f"Archive complete: {archive_path.name} ({compressed_gb:.2f} GB compressed)")

    return {
        'path': str(archive_path),
        'source_files': archived_files,
        'source_bytes': done_bytes,
        'vanished_entries': vanished,
        'compressed_bytes': archive_path.stat().st_size,
    }


@task(name="verify-archive")
def verify_archive(backup: Dict) -> Dict:
    """
    Read the entire archive back.

    A truncated or CRC-damaged gzip still has a plausible file size, so the old
    `size > 0` check would pass it. Streaming every member forces gzip to validate
    its CRC32 and is the only way to know the archive is actually restorable.
    """
    logger = get_run_logger()
    archive_path = Path(backup['path'])

    if not archive_path.exists() or archive_path.stat().st_size == 0:
        raise RuntimeError(f"Archive missing or empty: {archive_path}")

    read_files = 0
    read_bytes = 0
    with tarfile.open(archive_path, "r:gz") as tar:
        for member in tar:
            if not member.isfile():
                continue
            handle = tar.extractfile(member)
            if handle is None:
                raise RuntimeError(f"Unreadable member in archive: {member.name}")
            # Stream in chunks — some column files are large and this runs in a
            # memory-capped container.
            while chunk := handle.read(4 * 1024 * 1024):
                read_bytes += len(chunk)
            read_files += 1

    if read_files != backup['source_files']:
        raise RuntimeError(
            f"Archive incomplete: {read_files:,} files readable but "
            f"{backup['source_files']:,} were archived"
        )
    if read_bytes != backup['source_bytes']:
        raise RuntimeError(
            f"Archive size mismatch: {read_bytes:,} bytes read vs "
            f"{backup['source_bytes']:,} archived"
        )

    logger.info(f"Archive verified: {read_files:,} files, {read_bytes/1e9:.2f} GB "
                f"read back with valid CRC")
    return {'verified_files': read_files, 'verified_bytes': read_bytes}


@task(name="write-manifest")
def write_manifest(backup_name: str, manifest: Dict, backup: Dict, verified: Dict) -> str:
    logger = get_run_logger()
    manifest_path = BACKUP_DIR / f"{backup_name}.manifest.json"

    manifest = dict(manifest)
    manifest['archive'] = {
        'name': f"{backup_name}.tar.gz",
        'source_files': backup['source_files'],
        'source_bytes': backup['source_bytes'],
        'vanished_entries': backup.get('vanished_entries', 0),
        'compressed_bytes': backup['compressed_bytes'],
        'verified_files': verified['verified_files'],
        'verified_bytes': verified['verified_bytes'],
    }

    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Manifest written: {manifest_path.name}")
    return str(manifest_path)


def _manifest_for(archive: Path) -> Path:
    return archive.with_suffix('').with_suffix('.manifest.json')


@task(name="cleanup-old-backups")
def cleanup_old_backups(keep: int):
    """
    Rotate archives, counting only those that completed.

    An archive without its manifest never reached the verification step, so it is
    unproven at best and truncated at worst. Such orphans are removed outright
    rather than being allowed to occupy a retention slot and push out a backup that
    was actually verified.
    """
    logger = get_run_logger()
    archives = sorted(BACKUP_DIR.glob("questdb_backup_*.tar.gz"))

    complete = []
    for archive in archives:
        if _manifest_for(archive).exists():
            complete.append(archive)
        else:
            archive.unlink()
            logger.warning(f"Deleted orphan archive with no manifest: {archive.name}")

    for old in complete[:-keep] if keep else complete:
        old.unlink()
        sidecar = _manifest_for(old)
        if sidecar.exists():
            sidecar.unlink()
        logger.info(f"Deleted {old.name}")

    logger.info(f"Keeping {min(len(complete), keep)} verified backup(s) in {BACKUP_DIR}")


@flow(name="questdb-local-backup", log_prints=True)
def questdb_backup_flow(keep_local_backups: int = 5, deep_check: bool = True):
    logger = get_run_logger()
    backup_name = f"questdb_backup_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    logger.info(f"Starting backup: {backup_name}")

    # Gate first — never archive a database that failed verification.
    manifest = verify_integrity(deep=deep_check)

    # Pin a consistent snapshot around the tar, and always release it — even if the
    # archive step raises — so a crashed backup never leaves a snapshot held.
    snapshot_prepare()
    try:
        backup = create_backup(backup_name)
        verified = verify_archive(backup)
    finally:
        snapshot_complete()

    manifest_path = write_manifest(backup_name, manifest, backup, verified)
    cleanup_old_backups(keep_local_backups)

    logger.info(f"Done — archive and manifest in {BACKUP_DIR}")
    logger.info(f"  verify a restore with: "
                f"python db/integrity_check.py --verify {Path(manifest_path).name}")
