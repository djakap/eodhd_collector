# Table Authority

Status: **OBSERVED on 2026-09-11; corporate-action completeness remains open.**

"Production" means the table written by active Prefect schedules and read by
downstream consumers at the measurement date. It does not mean that every row
has been independently verified complete.

## Price tables

| Role | Table | Rows | Symbols | First | Last |
|---|---|---:|---:|---|---|
| Production | `stock_data` | 4,880,796 | 651 | 2000-03-01 | 2026-09-11 09:00 |
| Legacy EODHD | `eodhd_stock_data` | 21,299,141 | 868 | 1989-09-19 | 2026-07-23 09:10 |

`stock_data` is the active production price table. `eodhd_stock_data` is a
frozen legacy/source-specific table retained for archive, reconciliation, and
historical coverage.

Price-coverage metadata follows the same authority split. Production coverage
is stored in `stock_metadata`; `eodhd_stock_metadata` is frozen legacy evidence.
Their grain and freshness semantics are defined in
[`METADATA_SEMANTICS.md`](METADATA_SEMANTICS.md).

## Corporate-action tables

| Operational role | Table | Rows | Symbols | First action | Last action |
|---|---|---:|---:|---|---|
| Current production writer | `corporate_actions` | 4,523 | 480 | 2000-06-27 | 2026-09-10 |
| Legacy EODHD | `eodhd_corporate_actions` | 4,585 | 487 | 1995-06-02 | 2026-07-21 |

Neither corporate-action table is a superset of the other when compared by
`(symbol, action_type, action_date)`: 4,440 keys are shared, 83 exist only in
`corporate_actions`, and 145 exist only in `eodhd_corporate_actions`.

The corruption reported in the 2026-09-10 audit did not reproduce on
2026-09-11. All 27 `corporate_actions` partitions materialised, no anomalous
partition was observed, and the partition row total equalled the table count.
No cause was established for the difference from the audit finding.

This operational naming does not settle completeness or justify repointing a
consumer. Reconciliation and the final corporate-action authority decision
belong to QCF-013.
