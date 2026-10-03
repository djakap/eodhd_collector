# QCF-004 adjustment fixtures

These fixtures were extracted on 2026-10-03 from the production backup
`questdb_backup_2026-09-27_125240`, restored read-only on MAC. The snapshot's
`max(stock_data.created_at)` is `2026-09-27T12:39:40Z` (sub-second value omitted here as in the task
specification).

The extraction ran as a throwaway Python script through `eodhd_collector-test`; that script is not
committed. Both JSON files repeat the snapshot identity and query.

## `scale_1h.json`

The source query was:

```sql
WITH h AS (SELECT symbol, timestamp, last(close) AS hclose FROM stock_data
           WHERE interval = '1h' SAMPLE BY 1d ALIGN TO CALENDAR),
     d AS (SELECT symbol, timestamp, close AS dclose FROM stock_data WHERE interval = 'd')
SELECT h.symbol, h.timestamp, h.hclose, d.dclose
FROM h JOIN d ON h.symbol = d.symbol AND h.timestamp = d.timestamp
```

CUAN.JK, KDSI.JK and FISH.JK contain their complete joined histories. The clean cases are limited to
60 calendar days on either side of their recorded split: MLPT.JK 2026-07-21, DSSA.JK 2026-04-09,
RMKE.JK 2026-07-17, KLAS.JK 2024-11-11 and CYBR.JK 2026-05-13. MINA.JK covers 60 days before its
2025-06-30 run start through 60 days after its 2025-07-08 run end.

## `staleness.json`

The two actions were selected from:

```sql
SELECT symbol, action_type, action_date FROM corporate_actions
WHERE action_date > %s AND action_type IN ('split', 'dividend') ORDER BY action_date, symbol
```

For IKBI.JK's 2026-08-11 dividend and CMRY.JK's 2026-09-04 dividend, each `d`, `w` and `m` group came
from:

```sql
SELECT created_at, count() FROM stock_data
WHERE symbol = %s AND interval = %s AND timestamp < %s
```

The cutoff is seven days before the action. Tests apply the write-time predicate to these raw
`created_at` groups.
