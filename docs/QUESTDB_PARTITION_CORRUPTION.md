# QuestDB partition-version corruption diagnosis

## 1. Evidence used and evidence missing

| Label | Evidence | What it establishes or would decide |
|---|---|---|
| OBSERVED | The restored `questdb_backup_2026-09-27_125240` snapshot in the Mac Docker volume | The current snapshot has 14 user tables, 835 partitions in its pinned manifest, no `numRows > 0 / diskSize == 0` partition, and an empty `sys.column_versions_purge_log`. It establishes snapshot state after the recorded repairs, not state at an incident. |
| OBSERVED | The 101 `crash+*.log` / `hs_err*.log` files in the snapshot's read-only `db/` directory | Signal, `si_code`, first QuestDB Java frame, UTC crash time, and JVM uptime can be reproduced with `scripts/classify_questdb_crashes.py`. |
| UNVERIFIED | `quant-platform/planning/tasks/QCF-013.md`, `planning/implementations/QCF-013.md`, and `planning/reviews/QCF-013.md` | These are the surviving records for the 2026-09-12 incident and restore. Their underlying production runtime and evidence directory are not on Mac, so their incident facts are not re-measured here. |
| UNVERIFIED | `quant-platform/docs/CURRENT_STATE.md`, section “2026-09-27 — QuestDB recovery and backup restored” | This is the surviving record for the later `corporate_actions`, quarantine-table, and `yf_fundamentals` damage. The Linux evidence files it cites are absent from Mac. |
| UNVERIFIED | `docs/CORPORATE_ACTIONS_RECOVERY.md` | This records the exercised 2026-09-15 restore and the then-OPEN cause. Its treatment of `_todo_` as incident evidence is superseded by the later cross-table measurement in `quant-platform/docs/CURRENT_STATE.md`: `_todo_` is a normal per-table counter and not a corruption signature. |
| UNVERIFIED | Production's live QuestDB, container logs, restart history, and incident-time table files | A same-night capture would decide whether a transaction committed a partition version that never reached disk, whether a purge removed the referenced version, and whether a crash/restart coincided with that transition. These sources are Linux-only and were not contacted. |
| UNVERIFIED | Production Prefect flow-run history | This would decide which writer, backup, or maintenance flow overlapped each partition-version transition. It is Linux-only and was not contacted. |
| UNVERIFIED | Windows/WSL2 sleep, shutdown, storage, and power logs | These would decide whether a host suspend, forced shutdown, storage error, or disk-pressure event coincided with each transition. They are Linux-host evidence and were not contacted. |
| UNVERIFIED | `backups/evidence/` copies captured around the incidents | Their `_txn`, `_cv`, partition-directory, and file-time state would allow an incident-version reference to be compared with the directories that actually existed. The copies are Linux-only and were not transferred to Mac. |

## 2. Incident timelines

### 2.1 `corporate_actions`, detected 2026-09-12

- UNVERIFIED — The 2026-09-10 archive later restored cleanly as 4,523 rows in 27 yearly partitions through 2026-09-10. Source: `quant-platform/planning/implementations/QCF-013.md`, Step C.
- UNVERIFIED — Four measurements on 2026-09-11 and the morning of 2026-09-12 found the table clean. Exact timestamps for those measurements are not retained in the Mac records. Source: `quant-platform/planning/tasks/QCF-013.md` § 3.2.
- UNVERIFIED — At 2026-09-12 04:16:13 UTC the retained pre-restart log recorded `corporate_actions` partition merges for 2023–2026 and a switch to `2026.5136`; its last line at 04:16:18 UTC began another insert. The record does not contain the event that ended that process. Source: `quant-platform/planning/tasks/QCF-013.md` § 3.2.
- UNVERIFIED — The QuestDB container subsequently had `StartedAt=2026-09-12T08:38:07Z` and `RestartCount=0`; the record interprets this as consistent with a Docker Desktop/WSL2 stop-and-resume, not as proof of one. Source: `quant-platform/planning/tasks/QCF-013.md` § 3.2 and `planning/implementations/QCF-013.md`, Step 0.
- UNVERIFIED — The table `_txn` file was recorded at 08:38:38 UTC and `_todo_` at 08:53:11 UTC. The later cross-table inspection shows `_todo_` is not a corruption marker, so its timestamp does not establish recovery activity or causation. Source: `quant-platform/planning/tasks/QCF-016.md` § 3.5 and `quant-platform/docs/CURRENT_STATE.md`, `_todo_` correction.
- UNVERIFIED — At 2026-09-12 17:00–17:15 WIB, metadata claimed 3,826 rows in 26 partitions through 2025, `count()` read 27 rows, the WAL was not suspended (`writerTxn=sequencerTxn=29889`), and on-disk partition years stopped at 2017. Source: `quant-platform/planning/tasks/QCF-013.md` § 3.1.
- UNVERIFIED — On 2026-09-15 at 00:43 UTC, the scheduled backup failed while reading missing partition 2002. Source: `quant-platform/planning/implementations/QCF-013.md`, Step 0.
- UNVERIFIED — On 2026-09-15, the damaged directory was preserved and the clean 2026-09-10 archive was restored under the production table name. The root cause remained OPEN after post-restart stdout, `_todo_`, and crash-log searches. Source: `quant-platform/planning/implementations/QCF-013.md`, Steps A–D; `planning/reviews/QCF-013.md`, AC7.

### 2.2 `corporate_actions`, damaged again by 2026-09-26

- UNVERIFIED — By 2026-09-26, partitions 2005–2016 were again unreadable, accounting for 1,663 rows. The Mac record does not retain their incident `_txn` version, directory-version listing, purge log, writer overlap, container log, restart history, or host power/storage history. Source: `quant-platform/docs/CURRENT_STATE.md`, 2026-09-27 “What was wrong”.
- UNVERIFIED — On 2026-09-27, a human-authorized operation dropped those 12 partitions; the remaining table then agreed at 2,863 readable and metadata rows across 15 partitions. Source: `quant-platform/docs/CURRENT_STATE.md`, 2026-09-27 “Actions taken”.
- OBSERVED — The restored Mac snapshot reproduces that post-repair state: 2,863 readable and metadata rows across 15 partitions, with no zero-disk partition. This observation cannot reconstruct the missing incident versions.

### 2.3 `corporate_actions_damaged_20260912`, assessed 2026-09-27

- UNVERIFIED — The 2026-09-12 damaged table was retained under `corporate_actions_damaged_20260912` after the 2026-09-15 swap. Source: `quant-platform/planning/implementations/QCF-013.md`, Step A.
- UNVERIFIED — On 2026-09-27, 24 of its 26 metadata partitions were recorded as corrupt, with 27 rows readable out of 3,826 claimed. The Mac record does not retain a per-version timeline showing whether this was new damage after the rename or a fuller classification of the already-damaged table. Source: `quant-platform/docs/CURRENT_STATE.md`, 2026-09-27 “What was wrong”.
- UNVERIFIED — After a filesystem evidence copy was verified, the quarantine table was dropped with human authorization. That evidence directory is not present on Mac. Source: `quant-platform/docs/CURRENT_STATE.md`, 2026-09-27 “Actions taken”.

### 2.4 `yf_fundamentals`, detected 2026-09-27

- UNVERIFIED — On 2026-09-27, partitions 2024 and 2025 were recorded as corrupt. For 2025, 12.6 MB of data existed in `2025.624868` while `_txn` referenced a version absent from disk; the 2024 referenced directory was absent. Source: `quant-platform/docs/CURRENT_STATE.md`, 2026-09-27 “Root cause — still OPEN”.
- UNVERIFIED — No flow was running at the recorded marker times and the QuestDB container reported zero policy restarts. This rules out neither host termination nor an unlogged engine failure. Source: `quant-platform/docs/CURRENT_STATE.md`, 2026-09-27 “Root cause — still OPEN”.
- UNVERIFIED — The two partitions were dropped and recollected with human authorization. The recollection process stopped after 9.7 hours when the laptop slept, but a later full scan found no remaining zero-disk signature. Source: `quant-platform/docs/CURRENT_STATE.md`, 2026-09-27 “Still open after this session”.
- OBSERVED — The Mac snapshot predates that recollection and contains 196,629 `yf_fundamentals` rows with metadata count equal to `count()`, four partitions, and no zero-disk partition. It is post-incident archive evidence, not a reproduction of the corrupt state.
- OBSERVED — `sys.column_versions_purge_log` has zero rows in the Mac snapshot. This shows only that no purge record survived into the backup; it cannot support or refute an incident-time purge.

## 3. Candidate mechanisms

| Label | Candidate | Status | Evidence | Single Linux observation that would decide it |
|---|---|---|---|---|
| HYPOTHESIS | Interrupted column-version purge | untestable-from-Mac | The incident signature is consistent with metadata referencing a version no longer on disk, while the restored snapshot's purge log is empty. An empty post-repair log contains no incident-time ordering evidence. | Capture `sys.column_versions_purge_log` plus `_txn`/column-version state and partition-directory inventory before repair at the next alert; a matching incomplete purge entry for the missing version would support it, while a complete non-matching log would refute that occurrence. |
| HYPOTHESIS | Crash between writing a partition version and committing its reference | untestable-from-Mac | The 2026-09-12 record ends during writes and resumes with divergent metadata/directories, but it lacks the boundary log or transaction/file ordering needed to establish which side committed. | Preserve engine logs and filesystem metadata immediately at the alert; a committed `_txn` reference whose target directory was never durably created would support it. |
| HYPOTHESIS | SIGBUS caused by a mapped file being truncated or removed underneath the process | untestable-from-Mac for the September incidents | OBSERVED: 84 files report `SIGBUS/BUS_ADRERR`, and 81 first enter QuestDB in compiled filter code. UNVERIFIED: none overlaps the September incident dates in the evidence retained on Mac. | A same-time JVM crash/core whose mapped fault address resolves to the partition-version file that disappears would support the link; an incident without such a crash would refute it for that occurrence. |
| HYPOTHESIS | Host sleep, forced WSL2 shutdown, or WSL2 I/O behavior interrupts a merge/purge | untestable-from-Mac | The 2026-09-12 record is consistent with a stop-and-resume, and a later recollection ended when a laptop slept without recreating the signature. Neither observation establishes the host event at a corruption transition. | Correlate an incident alert to Windows/WSL2 suspend/shutdown and storage logs, with a clean pre-event inventory and divergent post-event inventory. |
| HYPOTHESIS | Disk pressure or storage failure prevents a partition version from reaching durable storage | untestable-from-Mac | No incident-time free-space, `ENOSPC`, filesystem-error, or block-I/O record is present on Mac. JVM native-memory allocation failures are not evidence of disk exhaustion. | Retain host free-space and kernel/storage errors at the alert; an `ENOSPC` or I/O failure on the affected path at the version transition would support it. |
| OBSERVED | SIGBUS in JIT-compiled filter code | supported as a crash class; untestable as the corruption cause | 81 of 101 files have `io.questdb.jit.FiltersCompiler.callFunction` as their first QuestDB Java frame, and all 81 are among the SIGBUS files. The files end on 2026-06-05, before the recorded September incidents. | Capture the triggering SQL, core/memory map, and affected table at a recurrence; a faulting mapping belonging to the subsequently missing partition would link the crash class to corruption. |

## 4. Crash classification and incident correlation

- OBSERVED — `scripts/classify_questdb_crashes.py` found 101 files: 84 `SIGBUS/BUS_ADRERR`, 3 `SIGSEGV/SEGV_MAPERR`, and 14 without a fatal-signal or `si_code` line.
- OBSERVED — First QuestDB frames were: 81 `FiltersCompiler.callFunction`, 3 `Rosti.keyedIntCount`, 2 `TableTransactionLog$TransactionLogCursorImpl.getWalId`, 1 `WalTxnDetails.readObservableTxnMeta`, 2 `BytecodeAssembler.newInstance`, 1 `WalWriterPool.newTenant`, and 11 files with no QuestDB Java frame.
- OBSERVED — UTC file dates were 2026-02-12 (10), 02-13 (3), 02-21 (4), 05-31 (8), 06-01 (9), 06-02 (66), and 06-05 (1).
- OBSERVED — All 101 files identify Corretto 17.0.7.7.1, and the QuestDB frames identify module version 7.3.10.
- OBSERVED — JVM uptime at failure has a median of 3.861219 seconds and a maximum of 32,820.580099 seconds. The 66 failures on 2026-06-02 therefore form a startup-heavy crash cluster.
- OBSERVED — The newest retained crash is dated 2026-06-05; the first recorded partition incident is dated 2026-09-12. There is no temporal correlation in the files available on Mac, so they are not direct records of the September incidents.
- UNVERIFIED — The incident record says QuestDB stops rotating after 100 numbered crash files and would overwrite `hs_err_pid+1.log`; that rotation message is not present in the 101 crash files themselves. A post-2026-06-05 crash is therefore not proven or excluded by this set.
- HYPOTHESIS — The repeated SIGBUS/JIT pattern may expose the same underlying file-lifecycle fault, but the three-month evidence gap prevents promoting that association to a cause.

## 5. Root cause

- OBSERVED — The recurring failure signature is a partition-version reference that does not resolve to readable on-disk data: a missing `<partition>.<txn>` directory or `numRows > 0` with `diskSize == 0`. `_todo_` is not part of that signature.
- UNVERIFIED — The surviving incident records do not contain a complete causal sequence from writer/purge action through transaction commit, directory removal, host event, and first failed read.
- HYPOTHESIS — Root cause is **OPEN**. Interrupted purge, commit/durability ordering, mapped-file truncation, and host/storage interruption remain viable mechanisms; none is established by the Mac evidence.
- HYPOTHESIS — Establishing the cause requires preserving, before repair, the alerting table's `table_partitions()` rows including `diskSize`, WAL transaction state, `_txn` and column-version metadata, versioned directory listing and timestamps, `sys.column_versions_purge_log`, QuestDB logs/crash/core data, overlapping Prefect runs, and host power/storage events.

## 6. Mitigations not applied

- HYPOTHESIS — Disabling QuestDB SQL JIT could avoid the dominant `FiltersCompiler` crash path, but it is a production configuration change with unmeasured performance and correctness effects. QCF-016 did not apply it.
- HYPOTHESIS — Changing QuestDB versions could remove an engine defect, but no evidence here identifies a fixed upstream defect or proves compatibility with this data directory. QCF-016 did not upgrade or migrate QuestDB.
- HYPOTHESIS — Retaining every crash/core file outside the 100-file rotation would improve the next correlation. This is an operational recommendation, not a change made by QCF-016.
- OBSERVED — QCF-016 performed no write, DDL, repair, restore, table drop, partition drop, container lifecycle action, Prefect action, or QuestDB configuration change.
- OBSERVED — The implemented nightly mitigation is detection and alerting: every user table is inspected, the zero-disk signature remains an alert even for `yf_fundamentals`, and only its known count divergence is logged without failing under explicit issue id `DATA-002`.
