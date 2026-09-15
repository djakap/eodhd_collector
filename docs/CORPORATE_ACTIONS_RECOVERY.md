# Production corporate-actions recovery

## Current authority and supported range

`corporate_actions` is the production corporate-actions table. On 2026-09-15 it
was restored from the verified 2026-09-10 QuestDB archive and checked across all
13 columns. Its declared supported range is **2000-06-27 through 2026-09-10**:
4,523 rows in 27 yearly partitions. The pre-incident count was also 4,523, so
the restore delta is zero.

The damaged table was not deleted. It is retained as
`corporate_actions_damaged_20260912`, and a separate read-only filesystem copy
is retained under:

```text
backups/evidence/qcf-013-corporate_actions-corrupt-20260915-pre-repair/
```

The combined content-manifest checksum of that evidence copy is:

```text
a6cae60110e7b511fec172bba4b0eb9dcc36f1b41472759fe88bd1470cedd867
```

`eodhd_corporate_actions` remains the untouched legacy/fallback table. It is not
the production writer target.

## Fault signature and detection

The damaged table had a metadata/directory divergence:

```text
table_partitions()       26 partitions / 3,826 rows / 2000-2025
SELECT count()           27 readable rows
filesystem               54 partition directories, ending at 2017
WAL                      suspended=false, writerTxn=sequencerTxn=29889
marker                   _todo_, mtime 2026-09-12 08:53:11 UTC
```

This is not a suspended-WAL incident. `scripts/heal_suspended_wal.py` correctly
handles suspended WAL segments, but cannot see a table whose WAL is fully
applied while its partition metadata and directories disagree.

`scripts/check_table_partitions.py` closes that detection gap by comparing the
sum of `numRows` from `table_partitions()` with a readable `count()`. It reports
the mismatch and never repairs it. Suspended tables are deferred to the existing
WAL heal path to avoid double-reporting.

The check is wired next to WAL healing in `flows/nightly_check_flow.py`. Worker
source is baked into its image, so this wiring is **committed but not yet live**.
It becomes live only after a human-authorized worker rebuild/recreate; QCF-013
did not build, restart, recreate, or otherwise change a container.

## Root-cause disposition

**OPEN — searched and not found.** The following evidence was inspected after
the damaged directory had been preserved:

- `log.conf`: the declared `file` writer is commented out; only stdout is active;
- container stdout from the 2026-09-12 08:38:07 UTC start: no recovery,
  `_todo_`, or corporate-actions event was present;
- `_todo_`: a 4,096-byte binary marker with `0x11` at offsets 0 and 24 and no
  explanatory text;
- QuestDB crash logs: the latest files predate the September incident and do
  not explain it.

The pre-restart log ending during a multi-partition yfinance action write remains
consistent with an interrupted merge, but no post-restart evidence establishes
that sequence. It is therefore not recorded as the cause.

## Restore procedure exercised on 2026-09-15

Backups must be verified on the host at
`/home/djp/quant/eodhd_collector/backups`. The worker's `/backup` bind mount is
currently detached and is not evidence that a backup is absent or present.

1. Re-measure the live fault and WAL state. Stop if the row count, `_todo_`, or
   partition inventory differs from the recovery plan.
2. Copy `corporate_actions~100` to a host evidence directory before reading
   recovery logs or changing the table. Make the copy non-writable and checksum
   it.
3. Inspect `questdb_backup_2026-09-10_081325.tar.gz` and its manifest on the
   host. Extract its `corporate_actions~100` into a scratch root and open that
   scratch root on isolated ports with the same QuestDB 7.3.10 runtime.
4. Verify the scratch table: 4,523 rows, 27 partitions, 2000-06-27 through
   2026-09-10, full projection readable, and `sum(numRows) == count()`.
5. Run the restore tool in its default dry-run mode:

   ```text
   python scripts/restore_production_corporate_actions.py \
     --source-port <scratch-pg-port> --target-port <live-pg-port>
   ```

6. Build and verify the replacement without renaming the live table:

   ```text
   python scripts/restore_production_corporate_actions.py --build \
     --source-port <scratch-pg-port> --target-port <live-pg-port>
   ```

7. Only after independent verification, swap the names:

   ```text
   python scripts/restore_production_corporate_actions.py --swap \
     --source-port <scratch-pg-port> --target-port <live-pg-port>
   ```

The tool refuses a non-empty build target. It uses only `CREATE TABLE`, row
inserts, and the two table renames. Do not substitute in-place partition surgery
for this procedure, even if a QuestDB error message suggests it.

The archive extraction and isolated query in steps 3-4 exercise the backup's
restore procedure rather than merely checking that the gzip file opens. The
ordered 13-column projections of scratch and restored live tables both hashed to
`3080296a9b33708bda0fd8beee9ccde8c59c60e4984237f196c6a852870f0fde`.

## Source reconciliation intentionally deferred

The last healthy comparison found 4,440 shared keys, 83 keys only in
`corporate_actions`, and 145 only in `eodhd_corporate_actions`. This restore
reproduced the archived production table exactly; it did not merge the two
providers or adjudicate their different values. That data-semantics
reconciliation remains a follow-up rather than being hidden inside recovery.

## Operational tooling review

- `scripts/heal_suspended_wal.py` is tracked and wired at worker startup and in
  the nightly flow. Its scope is intentionally suspended WAL, not this fault.
- `scripts/clear_orphan_runs.py` is tracked and wired only at worker startup. It
  repairs Prefect run-state/concurrency bookkeeping and is unrelated to table
  partitions; running it inside a flow would be unsafe.
- Both startup invocations remain best-effort (`|| true`). No recurring recovery
  path depends on an untracked local file.
- The 2026-09-15 `questdb-backup` run failed loudly while reading the corrupt
  table. Restoring `corporate_actions` removes that data blocker. Re-establishing
  the detached `/backup` mount still requires a separate human-authorized
  container recreate.
