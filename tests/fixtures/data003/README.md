# DATA-003 fixtures

The ten provider-response JSON files are byte-for-byte copies of
`quant-platform/planning/tasks/DATA-003-evidence/`, recorded by the DATA-003 planning diagnostic on
2026-10-04. Their SHA-256 values are:

```text
7bd02dbe1c00b9fb163f89a700fa38f33619652adf38dba527a16ef3d6d91a3d  CUAN.JK_1h.json
4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945  CUAN.JK_1h_edge.json
f48a3ed2350f8c93c703054d2f759368cc6e04a3f4823307e6a32f09bc7b2aec  CUAN.JK_actions.json
92d102757048b5b4d5eec5165ab205aa90b168ebbb4e161babbdeed3a1cfc905  FISH.JK_1h.json
06cf61dc29dbf259c1d3d7592305bfaffc4d6958c7d7a90c8486fe34fc128290  FISH.JK_actions.json
7017153941042da181be80824ecef709aa4b1320166f2fd6c188d8f1919cac4f  KDSI.JK_1h.json
327ca11fc8841778e4a37b0b95842510cc85a07a115d905a1baa931f46639bfb  KDSI.JK_actions.json
e7448d35bf76a730ff590a6c1425ffab712aea7cccdded584159bd5f1035bccb  MLPT.JK_1h.json
d68c75cfd0ca88e31ba5387a0e6e0d33e71de561b5a7188899c022e61e682321  MLPT.JK_actions.json
3f40d3c3f80d0dad551400905749e808a71f15fcac0c0611703ebb6ee3235753  meta.json
```

`stored_1h.json` and `splits.json` were extracted read-only from the restored production backup of
2026-09-27 on MAC. At extraction, `max(stock_data.created_at)` was
`2026-09-27T12:39:40.343102` (`2026-09-27T12:39:40Z` at the task's second-level snapshot
identifier).

Stored 1h query, once per symbol/range:

```sql
SELECT timestamp, open, high, low, close FROM stock_data
WHERE symbol = %s AND interval = '1h' AND timestamp >= %s AND timestamp < %s
ORDER BY timestamp
```

Ranges are CUAN.JK `2024-08-20..2025-08-16`, KDSI.JK `2024-08-21..2024-12-16`, FISH.JK
`2024-08-20..2025-10-16`, and MLPT.JK `2026-05-01..2026-08-16` (end-exclusive).

Split query:

```sql
SELECT symbol, action_date, split_ratio FROM corporate_actions
WHERE action_type = 'split' AND symbol IN (%s, %s, %s, %s)
ORDER BY symbol, action_date
```

The extracted fixture hashes are:

```text
838d1627312a60a82f8289fbfe63d124ae126f18fe5454421823ab475a6b34b0  stored_1h.json
fae6ed61d52bf3dde87d88c577dc062139634ff74f938ca2fec2920bcb5a8d96  splits.json
```
