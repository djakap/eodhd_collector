# Price adjustment consistency

**Split and dividend adjustment is provider behaviour, not a platform promise.** The store holds
whatever price basis Yahoo served when each row was last written. `created_at` records that last
write; it does not make older provider history immutable.

## Collection windows

The regular collector re-fetches only a recent window. `YF_UPDATE_WINDOW_DAYS = 7` applies to
`d`, `w`, `m` and `1h`; the hourly flow uses `LOOKBACK_DAYS = 3` before deriving `4h` again. The
full-history backfill is manual-only. Rows older than those windows keep the basis served by Yahoo
when they were last written.

## Observed consequence

On the production backup of 2026-09-27, 15 dividends had occurred since 2026-07-21. Every one had
older rows written before the action. Their dividend-adjusted histories can therefore retain the old
basis beyond the short re-fetch window.

For a future split, the same source and collection-window behaviour predicts that older `close`
values will retain the pre-split scale while the recent window moves to the newly served basis. This
is a prediction: the live incremental path had not crossed a split on the measured snapshot.

Three stored series are already inconsistent across intervals:

| Symbol | Intervals on the inconsistent scale | Span | Interval/daily ratio |
|---|---|---|---|
| CUAN.JK | `1h`, `4h` | 2024-08-20 through 2025-07-09 (207 days) | 0.1000 |
| KDSI.JK | `1h`, `4h` | 2024-08-21 through 2024-11-04 (30 days) | 0.2500 |
| FISH.JK | daily is inconsistent with `1h`/`4h` | 2024-08-20 through 2025-01-15 (63 joined days) | interval/daily = 0.1000 |

## Nightly check

`scripts/check_adjustment_consistency.py` is read-only and uses two predicates:

- The scale predicate compares the last `1h` or `4h` close of each day with that day's daily close.
  A day is off-scale beyond 1%; a finding requires at least 15 days whose ratio remains within 1%
  of the run's first ratio. On the snapshot, the longest unrelated run was 8 days and the shortest
  known defect was 30 days.
- The staleness predicate examines actions after 2026-07-21 and counts rows at least seven days
  older than the action whose last write predates it. This found stale rows for all 15 measured
  dividends.

`KNOWN_SCALE_RUNS` registers the six symbol/interval runs above, and
`KNOWN_STALENESS_CLASSES` registers dividend staleness as a class. Each entry means the defect is
known and tracked by DATA-003; it does not mean the data is correct. A new finding, a split-staleness
finding, or a registered run that disappears is a problem and fails the nightly flow.

The scale check cannot see a factor below 1.01. The staleness check intentionally over-reports a
HUMI-style case where rows predate an action but the provider did not change their values. That is
the safe direction for this guard.

Run it in the worker environment with:

```text
python scripts/check_adjustment_consistency.py
```

Exit `0` means every finding is registered; exit `1` means at least one problem is unregistered or
a registered scale run has vanished.

## Repair

DATA-003 owns re-fetching and repair. Until then, treat a dividend-adjusted series across a recent
dividend, and any series across a recent split, as suspect.
