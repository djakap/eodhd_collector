# EODHD Collector - Available Commands

## Basic Usage

```bash
# Run collector for first 50 stocks
./run_collector.sh

# Or with conda environment
conda activate tradingStrategy
python main.py --stocks config/syariah_stocks.txt --limit 50
```

## Command-Line Options

### Required
- `--stocks PATH` - Path to stock list file (e.g., `config/syariah_stocks.txt`)

### Data Collection Options
- `--skip-price` - Skip price data collection
- `--skip-actions` - Skip corporate actions collection (dividends, splits)
- `--intraday-days DAYS` - Days of intraday data to collect (default: 600)
- `--delay SECONDS` - Delay between stocks in seconds (default: 0.5)
- `--limit N` - Limit number of stocks to process (useful for testing)

### Advanced Options (Update Mode)
- `--update-mode` - **Incremental update**: Only process stale stocks (checks metadata)
- `--max-age DAYS` - Max age in days before stock is considered stale (default: 1)
- `--update-window DAYS` - Days to re-fetch for corrections (default: 7)
- `--force-update` - Force full re-fetch (ignore update-mode)
- `--skip-intraday` - Skip intraday data (EOD only, FASTEST)
- `--enable-validation` - Enable OHLC validation (slower but safer, default: disabled)

## Common Usage Examples

### 1. Initial Collection (First 3 stocks for testing)
```bash
./main_ultrafast.py --stocks config/syariah_stocks.txt --limit 3
```

### 2. Daily Update Mode (Incremental - Only stale stocks)
```bash
./run_update.sh
# Or manually:
./main_ultrafast.py --stocks config/syariah_stocks.txt --update-mode --max-age 1 --update-window 7
```

### 3. Full Collection (All stocks)
```bash
./run_ultrafast.sh
```

### 4. EOD Only (Fastest)
```bash
./main_ultrafast.py --stocks config/syariah_stocks.txt --skip-intraday
```

### 5. Force Full Update (Ignore metadata)
```bash
./main_ultrafast.py --stocks config/syariah_stocks.txt --update-mode --force-update
```

### 6. Force Update All Stocks (Re-download everything)
```bash
python main.py --stocks config/syariah_stocks.txt --force-update
```

### 7. Quick Test (10 stocks, skip actions)
```bash
python main.py --stocks config/syariah_stocks.txt --limit 10 --skip-actions
```

## Notes

- The collector automatically saves progress, so you can resume if interrupted
- Use `--update-mode` for daily updates to only fetch new data
- Use `--force-update` if you need to re-download all historical data
- Progress and stats are saved to the `data/` directory
- Logs are saved to `logs/eodhd_collector.log`
