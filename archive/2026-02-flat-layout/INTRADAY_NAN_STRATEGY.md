# Intraday NaN Filtering Strategy

## Overview
Modified intraday data collection to implement smart NaN filtering based on market hours.

## New Behavior

### ✅ **During Market Hours (09:00-16:00 Jakarta Time)**
- **Keep NaN values** as NULL in database
- **Purpose**: Preserve trading breaks and periods with no trades
- **Use case**: Accurate representation of actual market activity
- **Database**: Stored as NULL (not 0.0)

### ❌ **Outside Market Hours (Before 09:00 or After 16:00)**
- **Remove NaN values** completely
- **Purpose**: Eliminate after-hours noise
- **Use case**: Clean dataset without market-closed periods

## Implementation Details

### Market Hours Detection
```python
hour = timestamp.hour  # Jakarta time (UTC+7)
is_market_hours = 9 <= hour < 16
```

### NULL Handling
- **Market hours NULL**: Stored as `NULL` (true NaN)
- **After-hours NULL**: Skipped entirely (not stored)

## Benefits

1. **Preserves Trading Patterns**
   - Trading breaks visible in data
   - No-trade periods identifiable
   - Realistic market behavior

2. **Clean Dataset**
   - No after-hours noise
   - Only relevant trading hours data
   - Smaller database size

3. **Analysis Friendly**
   - Can identify low-liquidity periods
   - Can detect trading halts
   - Can analyze market microstructure

## Example

**Before (old logic):**
- All NaN records removed
- Trading breaks invisible
- Gaps in data

**After (new logic):**
```
09:00 - OHLC: 10000, 10100, 9900, 10050 ✅
09:05 - OHLC: NULL, NULL, NULL, NULL ✅ (trading break, kept as NULL)
09:10 - OHLC: 10050, 10150, 10000, 10100 ✅
...
16:00 - OHLC: 10200, 10250, 10150, 10200 ✅
16:05 - NaN ❌ (after hours, removed)
```

## Usage

The change is automatic. Just run intraday collection as normal:
```bash
python main_ultrafast.py --stocks config/syariah_stocks.txt --intraday-days 600 --skip-actions
```

## Notes

- Jakarta Stock Exchange (IDX) trading hours: 09:00-16:00
- Lunch break (11:30-14:00) NaN values will be preserved
- Pre-market and after-market NaN values will be removed
