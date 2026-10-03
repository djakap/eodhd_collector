"""Regression coverage for QCF-017 backup identity and transport."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

import db.integrity_check as integrity


ROOT = Path(__file__).resolve().parents[1]


class _Cursor:
    def close(self):
        pass


class _Connection:
    def cursor(self):
        return _Cursor()


def _fingerprint(partition="2026", **changes):
    value = {
        "partition": partition,
        "rows": 10,
        "min_timestamp": "2026-01-01 00:00:00",
        "max_timestamp": "2026-12-31 00:00:00",
        "checksum": 123.0,
        "disk_bytes": 4096,
    }
    value.update(changes)
    return value


def _entry(*fingerprints, **changes):
    value = {
        "rows": sum(fp["rows"] for fp in fingerprints),
        "timestamp_column": "ts",
        "partitions": len(fingerprints),
        "fingerprints": list(fingerprints),
    }
    value.update(changes)
    return value


def _verify(tmp_path, monkeypatch, expected, actual, *, health_failure=False):
    path = tmp_path / "backup.manifest.json"
    path.write_text(json.dumps({"tables": {"prices": expected}}), encoding="utf-8")

    def fake_entry(_cur, table, ts_col, report, deep=True):
        assert table == "prices"
        assert ts_col == "ts"
        assert deep is True
        if health_failure:
            report.fail("synthetic health finding")
        if isinstance(actual, Exception):
            raise actual
        return actual

    monkeypatch.setattr(integrity, "_build_table_manifest_entry", fake_entry)
    return integrity.verify_manifest(_Connection(), str(path))


def test_t0_identical_manifest_passes(tmp_path, monkeypatch):
    expected = _entry(_fingerprint())
    report = _verify(tmp_path, monkeypatch, expected, expected)

    assert report.exit_code == 0
    assert report.critical == []


def test_t1_checksum_only_mismatch_fails_with_context(tmp_path, monkeypatch):
    expected = _entry(_fingerprint())
    actual = _entry(_fingerprint(checksum=124.0))
    report = _verify(tmp_path, monkeypatch, expected, actual)

    assert report.exit_code == 2
    assert any("prices" in msg and "2026" in msg and "checksum" in msg
               for msg in report.critical)


@pytest.mark.parametrize(
    ("expected", "actual", "message"),
    [
        (_entry(_fingerprint("2025")), _entry(), "partition '2025' missing"),
        (_entry(), _entry(_fingerprint("2027")), "extra partition '2027'"),
    ],
)
def test_t2_t3_missing_and_extra_partitions_fail(
    tmp_path, monkeypatch, expected, actual, message
):
    report = _verify(tmp_path, monkeypatch, expected, actual)

    assert report.exit_code == 2
    assert any(message in msg for msg in report.critical)


@pytest.mark.parametrize("field", ["min_timestamp", "max_timestamp"])
def test_t4_timestamp_mismatch_fails(tmp_path, monkeypatch, field):
    expected = _entry(_fingerprint())
    actual = _entry(_fingerprint(**{field: "2026-06-01 00:00:00"}))
    report = _verify(tmp_path, monkeypatch, expected, actual)

    assert report.exit_code == 2
    assert any(field in msg for msg in report.critical)


@pytest.mark.parametrize(
    ("expected", "actual", "message"),
    [
        (_entry(_fingerprint()), RuntimeError("relation does not exist"), "missing after restore"),
        (_entry(_fingerprint()), _entry(_fingerprint(), rows=11), "rows mismatch"),
        (_entry(_fingerprint()), _entry(_fingerprint(), partitions=2), "partitions mismatch"),
    ],
)
def test_t5_missing_table_and_count_mismatches_fail(
    tmp_path, monkeypatch, expected, actual, message
):
    report = _verify(tmp_path, monkeypatch, expected, actual)

    assert report.exit_code == 2
    assert any(message in msg for msg in report.critical)


def test_t6_disk_bytes_are_not_part_of_identity(tmp_path, monkeypatch):
    expected = _entry(_fingerprint(disk_bytes=4096))
    actual = _entry(_fingerprint(disk_bytes=8192))
    report = _verify(tmp_path, monkeypatch, expected, actual)

    assert report.exit_code == 0


def test_t7_live_health_findings_do_not_change_identity_verdict(tmp_path, monkeypatch):
    expected = _entry(_fingerprint())
    report = _verify(
        tmp_path,
        monkeypatch,
        expected,
        expected,
        health_failure=True,
    )

    assert report.exit_code == 0
    assert report.critical == []


def test_t8_run_and_verify_share_the_manifest_entry_builder(tmp_path, monkeypatch):
    expected = _entry(_fingerprint())
    calls = []

    class EntryCursor:
        def execute(self, statement):
            assert statement == 'SELECT count() FROM "prices"'

        def fetchone(self):
            return (10,)

    parts = [("2026", 10, 4096, True, False, None, None)]
    monkeypatch.setattr(integrity, "check_partitions", lambda *_args: parts)
    monkeypatch.setattr(
        integrity,
        "deep_read_scan",
        lambda *_args: [_fingerprint()],
    )
    assert integrity._build_table_manifest_entry(
        EntryCursor(), "prices", "ts", integrity.IntegrityReport()
    ) == expected

    class Cursor(_Cursor):
        def execute(self, statement):
            assert "FROM tables()" in statement

        def fetchall(self):
            return [("prices", "ts")]

    class Connection:
        def cursor(self):
            return Cursor()

    def fake_entry(_cur, table, ts_col, _report, deep=True):
        calls.append((table, ts_col, deep))
        return expected

    monkeypatch.setattr(integrity, "_build_table_manifest_entry", fake_entry)
    monkeypatch.setattr(integrity, "check_wal_health", lambda *_args: None)
    monkeypatch.setattr(integrity, "check_timestamp_sanity", lambda *_args: None)
    monkeypatch.setattr(integrity, "check_partition_bounds", lambda *_args: None)
    monkeypatch.setattr(integrity, "check_table_bloat", lambda *_args: None)
    monkeypatch.setattr(integrity, "check_ohlc_validity", lambda *_args: None)

    generated = integrity.run_checks(Connection())
    assert generated.manifest["tables"]["prices"] == expected
    assert set(generated.manifest["tables"]["prices"]) == {
        "rows", "timestamp_column", "partitions", "fingerprints"
    }

    path = tmp_path / "backup.manifest.json"
    path.write_text(json.dumps({"tables": {"prices": expected}}), encoding="utf-8")
    verified = integrity.verify_manifest(Connection(), str(path))

    assert verified.exit_code == 0
    assert calls == [("prices", "ts", True), ("prices", "ts", True)]


def _write_docker_stub(path):
    path.write_text(
        """#!/bin/sh
set -eu
if [ "$1" = "inspect" ]; then
    echo true
    exit 0
fi
if [ "$1" != "exec" ]; then
    exit 2
fi
shift
shift
case "$1" in
    sh)
        for file in "$FAKE_BACKUP_DIR"/questdb_backup_*.tar.gz; do
            [ -e "$file" ] || continue
            echo "/backup/$(basename "$file")"
        done | sort
        ;;
    cat)
        cat "$FAKE_BACKUP_DIR/$(basename "$2")"
        ;;
    test)
        [ "$2" = "-f" ]
        [ -f "$FAKE_BACKUP_DIR/$(basename "$3")" ]
        ;;
    *)
        exit 2
        ;;
esac
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _copy_fixture(tmp_path):
    project = tmp_path / "project"
    scripts = project / "scripts"
    source = tmp_path / "container"
    bin_dir = tmp_path / "bin"
    scripts.mkdir(parents=True)
    source.mkdir()
    bin_dir.mkdir()
    shutil.copy2(ROOT / "scripts/copy_backup.sh", scripts / "copy_backup.sh")
    _write_docker_stub(bin_dir / "docker")
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["FAKE_BACKUP_DIR"] = str(source)
    return project, source, env


def _write_pair(source, stem):
    (source / f"{stem}.tar.gz").write_text(f"archive:{stem}", encoding="utf-8")
    (source / f"{stem}.manifest.json").write_text(f"manifest:{stem}", encoding="utf-8")


def test_t9_copy_script_moves_pairs_and_reports_missing_manifests(tmp_path):
    project, source, env = _copy_fixture(tmp_path)
    stem = "questdb_backup_2026-09-27_125240"
    _write_pair(source, stem)

    first = subprocess.run(
        [project / "scripts/copy_backup.sh"], env=env, text=True, capture_output=True
    )
    assert first.returncode == 0
    assert (project / f"backups/{stem}.tar.gz").exists()
    manifest = project / f"backups/{stem}.manifest.json"
    assert manifest.exists()

    manifest.unlink()
    second = subprocess.run(
        [project / "scripts/copy_backup.sh"], env=env, text=True, capture_output=True
    )
    assert second.returncode == 0
    assert manifest.exists()

    missing = "questdb_backup_2026-09-28_000000"
    (source / f"{missing}.tar.gz").write_text("orphan archive", encoding="utf-8")
    third = subprocess.run(
        [project / "scripts/copy_backup.sh"], env=env, text=True, capture_output=True
    )
    assert third.returncode != 0
    assert (project / f"backups/{missing}.tar.gz").exists()
    assert f"ERROR {missing}.manifest.json" in third.stdout


def test_t9_latest_copies_only_the_newest_pair(tmp_path):
    project, source, env = _copy_fixture(tmp_path)
    old = "questdb_backup_2026-09-27_000000"
    new = "questdb_backup_2026-09-28_000000"
    _write_pair(source, old)
    _write_pair(source, new)

    result = subprocess.run(
        [project / "scripts/copy_backup.sh", "--latest"],
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert not (project / f"backups/{old}.tar.gz").exists()
    assert not (project / f"backups/{old}.manifest.json").exists()
    assert (project / f"backups/{new}.tar.gz").exists()
    assert (project / f"backups/{new}.manifest.json").exists()


def test_t10_compose_uses_external_named_backup_volume():
    compose_text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "      - backups:/backup\n" in compose_text
    assert "./backups:/backup" not in compose_text
    assert (
        "  backups:\n"
        "    name: eodhd_collector_backups\n"
        "    external: true\n"
    ) in compose_text
