# Price Metadata Semantics

Status: **OBSERVED and implemented on 2026-09-12.** Production metadata describes
stored price coverage; it is not a substitute for querying price data when metadata
is absent.

## Tables and grain

| Role | Table | Grain | Coverage fields |
|---|---|---|---|
| Production | `stock_metadata` | one current row per `(symbol, interval)` in each write-day partition | `total_records`, `data_start`, `data_end` |
| Frozen legacy | `eodhd_stock_metadata` | historical rows per `(symbol, interval)` and collection day | fields contain the most recent legacy fetch batch, not total stored coverage |
| General legacy state | `eodhd_metadata` | one current-state row per symbol, with no interval | the price fields `total_price_records`, `earliest_price_date`, and `latest_price_date` are deliberately retired/unwritten |

`stock_metadata` is paired with production `stock_data`. Its three coverage values
are derived directly from all stored rows for the named symbol and interval:

```sql
SELECT count(), min(timestamp), max(timestamp)
FROM stock_data
WHERE symbol = $1 AND interval = $2;
```

The measurement before backfill on 2026-09-12 found 3,253 production
`(symbol, interval)` pairs across 4,880,908 price rows. The backfill method and
post-write verification are recorded in the QCF-002 implementation record.

## Why the legacy values are not totals

`eodhd_stock_metadata` is retained unchanged as historical evidence. Its writers
used the size and timestamp range of one fetched batch. For `AALI.JK`/`d`, its
latest row says 8 records from 2026-07-13 through 2026-07-22, while the paired
legacy price table actually contains 7,033 rows from 1997-12-09 through
2026-07-23. Production metadata must never copy those legacy figures: it derives
coverage from `stock_data` independently.

## Retired price fields on `eodhd_metadata`

`eodhd_metadata` has no interval column, so its `total_price_records` cannot
truthfully represent per-interval coverage. The price fields
`total_price_records`, `earliest_price_date`, and `latest_price_date` therefore
remain unwritten; the misleading batch-valued caller input was removed. This
decision is limited to those three price fields. Action, fundamental, and error
statistics on that schema have separate owners and remain outside QCF-002.

## Freshness has two meanings

- `last_updated` is metadata write time: when coverage was measured and persisted.
- `data_end` is data freshness: the timestamp of the latest stored price bar.

A recent `last_updated` cannot make an old `data_end` current. Conversely, an
absent or failed metadata write does not imply absent price data.

Metadata persistence is deliberately non-fatal. Collection continues if the
coverage query or metadata write fails, and the price table remains the source of
truth. Completeness and healing must not infer missing prices from missing
metadata; the active nightly completeness check queries `stock_data` directly.

## Known readers

The readers present when QCF-002 was implemented are all collector-internal legacy
paths:

- `QuestDBClient.get_max_timestamp` reads legacy `data_end` for incremental EODHD
  collection;
- `QuestDBClient.get_stocks_to_update` has no observed live caller;
- `QuestDBClient.check_data_freshness` is called by the legacy screener refresh;
- `scripts/legacy_eodhd/eodhd_data_validator.py` reports legacy
  `total_records`/`data_start` values;
- `flows/data_quality_flow.py` copies whole legacy metadata rows and its schedule is
  inactive.

The active `flows/nightly_check_flow.py` completeness path reads `stock_data`
directly and is unaffected.
