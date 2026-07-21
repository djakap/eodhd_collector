"""
The rules every writer into eodhd_stock_data must agree on.

This module exists because the same three rules were implemented independently in
four files and seventeen places, drifted apart, and produced most of the damage
found in the 2026-07-21 audit. Fixing them at the call sites never stuck; the next
writer reimplemented the old behaviour. They live here now, and callers import.

Three rules, each with the evidence behind it:

1. TIMESTAMPS ARE UTC-NAIVE.
   collectors/price_collector had three code paths for the same data. The pandas
   path used pd.to_datetime(unit='s'), which yields UTC. The numpy and loop
   fallbacks used datetime.fromtimestamp(), which yields the HOST's local time —
   WIB, seven hours off. Which convention a row got depended on which path
   happened to run, which is why rows landed outside market hours.

2. A BAR WITH NO OHLC IS NOT A BAR.
   The old rule kept or dropped empty rows based on the hour, comparing WIB market
   bounds (9..16) against UTC-stamped timestamps — exactly inverted, so it admitted
   the empty 16:00 WIB bar and would drop genuine ones. The hour test was never
   needed: an all-NULL row carries no information at any hour. Removing it removes
   the timezone dependency, and with it a whole class of bug.

   The test is deliberately narrow. Verified across all 27.8M rows on 2026-07-21:
   3,875,539 rows have all four OHLC fields NULL and NOT ONE of them carries
   volume, so dropping them loses nothing. But 1,274,415 rows have full OHLC with
   a NULL volume — EODHD genuinely returns volume: null — and those are real bars.
   A rule phrased as "reject rows containing a NULL" would destroy all of them.

3. THE INTERVAL MUST BE ONE WE KNOW.
   utils/aggregate_4h wrote 17,321 rows under the interval name '4h_null', every
   field NULL, and it went unnoticed for five months because nothing validated the
   column or enumerated its values.
"""

from datetime import datetime, timezone
from typing import Optional

# Interval codes accepted in eodhd_stock_data.interval. '4h' is derived locally by
# utils/aggregate_4h; the rest come from EODHD.
VALID_INTERVALS = frozenset({'5m', '15m', '30m', '1h', '4h', 'd', 'w', 'm'})

# Tuple layout used by QuestDBClient.insert_price_data.
IDX_INTERVAL, IDX_TIMESTAMP = 1, 2
IDX_OPEN, IDX_HIGH, IDX_LOW, IDX_CLOSE = 3, 4, 5, 6


def to_utc_naive(epoch_seconds) -> Optional[datetime]:
    """
    Epoch seconds -> naive UTC datetime, the storage convention.

    Never use datetime.fromtimestamp() for this: without a tz argument it resolves
    against the host's timezone, so the same input yields different rows on a WIB
    laptop and a UTC container.
    """
    if epoch_seconds is None:
        return None
    try:
        return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc) \
                       .replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


def is_empty_bar(open_, high, low, close) -> bool:
    """
    True when all four OHLC fields are absent — no information at any hour.

    Volume is deliberately not consulted: a bar with prices and a NULL volume is
    real and common (1,274,415 rows), while no all-NULL bar in the table carries
    volume, so consulting it would only risk false positives.
    """
    return open_ is None and high is None and low is None and close is None


def describe_rejection(record) -> Optional[str]:
    """
    Why this record should not be written, or None if it is fine.

    Returns a reason string rather than a bool so callers can count and log by
    cause. Silence is what let the previous defects accumulate: a gate that drops
    rows without saying so trades corruption for invisible data loss, which is the
    worse failure — corruption is detectable and repairable, absence is not.
    """
    try:
        interval = record[IDX_INTERVAL]
        ts = record[IDX_TIMESTAMP]
        o, h, l, c = (record[IDX_OPEN], record[IDX_HIGH],
                      record[IDX_LOW], record[IDX_CLOSE])
    except (IndexError, TypeError):
        return 'bentuk record tidak dikenal'

    if interval not in VALID_INTERVALS:
        return f'interval tidak dikenal: {interval!r}'
    if not isinstance(ts, datetime):
        return f'timestamp bukan datetime: {type(ts).__name__}'
    if ts.tzinfo is not None:
        return 'timestamp harus naive UTC, bukan tz-aware'
    if is_empty_bar(o, h, l, c):
        return 'bar kosong (OHLC seluruhnya NULL)'
    return None
