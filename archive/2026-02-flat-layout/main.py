"""
Main Orchestrator
Runs data collection for multiple stocks
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from datetime import datetime
import time
import logging

from collectors.price_collector import PriceCollector
from collectors.action_collector import ActionCollector
from utils.logger import setup_logging

logger = logging.getLogger(__name__)


def load_stocks(file_path: str) -> list:
    """Load stock symbols from file"""
    stocks = []
    with open(file_path, 'r') as f:
        for line in f:
            stock = line.strip()
            if stock and not stock.startswith('#'):
                # Add .JK suffix if not present
                if not stock.endswith('.JK'):
                    stock = f"{stock}.JK"
                stocks.append(stock)
    return stocks


def collect_single_stock(symbol: str, collect_price: bool = True, 
                        collect_actions: bool = True,
                        intraday_days: int = 600):
    """
    Collect data for a single stock
    
    Args:
        symbol: Stock symbol (e.g., 'BBCA.JK')
        collect_price: Whether to collect price data
        collect_actions: Whether to collect corporate actions
        intraday_days: Number of days for intraday data
    
    Returns:
        Dictionary with collection stats
    """
    stats = {
        'symbol': symbol,
        'price_stats': None,
        'action_stats': None,
        'success': False,
        'error': None
    }
    
    try:
        # Collect price data
        if collect_price:
            price_collector = PriceCollector()
            try:
                price_stats = price_collector.collect_all_intervals(symbol, intraday_days=intraday_days)
                stats['price_stats'] = price_stats
            finally:
                price_collector.close()
        
        # Collect corporate actions
        if collect_actions:
            action_collector = ActionCollector()
            try:
                action_stats = action_collector.collect_all_actions(symbol)
                stats['action_stats'] = action_stats
            finally:
                action_collector.close()
        
        stats['success'] = True
        
    except Exception as e:
        stats['error'] = str(e)
        logger.error(f"Failed to collect {symbol}: {e}")
    
    return stats


def collect_multiple_stocks(stocks: list, collect_price: bool = True,
                           collect_actions: bool = True,
                           intraday_days: int = 600,
                           delay: float = 0.5):
    """
    Collect data for multiple stocks
    
    Args:
        stocks: List of stock symbols
        collect_price: Whether to collect price data
        collect_actions: Whether to collect corporate actions
        intraday_days: Number of days for intraday data
        delay: Delay between stocks (seconds)
    
    Returns:
        List of collection stats
    """
    print(f"\n{'='*70}")
    print(f"🚀 EODHD DATA COLLECTION STARTED")
    print(f"{'='*70}")
    print(f"📊 Total stocks: {len(stocks)}")
    print(f"📈 Price data: {'Yes' if collect_price else 'No'}")
    print(f"💰 Corporate actions: {'Yes' if collect_actions else 'No'}")
    print(f"📅 Intraday days: {intraday_days}")
    print(f"{'='*70}\n")
    
    logger.info(f"Starting collection for {len(stocks)} stocks")
    logger.info(f"Price data: {collect_price}, Actions: {collect_actions}")
    logger.info(f"Intraday days: {intraday_days}")
    
    all_stats = []
    start_time = datetime.now()
    total_price_records = 0
    total_dividends = 0
    total_splits = 0
    
    for i, symbol in enumerate(stocks, 1):
        # Progress bar
        progress = i / len(stocks) * 100
        bar_length = 40
        filled = int(bar_length * i / len(stocks))
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f"\n[{i}/{len(stocks)}] {bar} {progress:.1f}%")
        print(f"{'='*70}")
        print(f"📍 Processing: {symbol}")
        print(f"{'='*70}")
        
        logger.info(f"\n{'='*70}")
        logger.info(f"Processing {i}/{len(stocks)}: {symbol}")
        logger.info(f"{'='*70}")
        
        stock_start = datetime.now()
        
        stock_stats = collect_single_stock(
            symbol,
            collect_price=collect_price,
            collect_actions=collect_actions,
            intraday_days=intraday_days
        )
        
        stock_elapsed = (datetime.now() - stock_start).total_seconds()
        all_stats.append(stock_stats)
        
        # Print summary
        if stock_stats['success']:
            price_records = stock_stats['price_stats']['total_records'] if stock_stats['price_stats'] else 0
            dividends = stock_stats['action_stats']['dividends'] if stock_stats['action_stats'] else 0
            splits = stock_stats['action_stats']['splits'] if stock_stats['action_stats'] else 0
            
            total_price_records += price_records
            total_dividends += dividends
            total_splits += splits
            
            print(f"✅ {symbol}: {price_records:,} price records, {dividends} dividends, {splits} splits ({stock_elapsed:.1f}s)")
            logger.info(f"✅ {symbol}: {price_records} price records, {dividends} dividends, {splits} splits")
        else:
            print(f"❌ {symbol}: {stock_stats['error']}")
            logger.error(f"❌ {symbol}: {stock_stats['error']}")
        
        # Show running totals
        elapsed = (datetime.now() - start_time).total_seconds()
        avg_time = elapsed / i
        eta = avg_time * (len(stocks) - i)
        
        print(f"\n📊 Running Totals:")
        print(f"   Price records: {total_price_records:,}")
        print(f"   Dividends: {total_dividends}")
        print(f"   Splits: {total_splits}")
        print(f"   Elapsed: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min")
        
        # Delay between stocks
        if i < len(stocks):
            time.sleep(delay)
    
    # Print final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    successful = sum(1 for s in all_stats if s['success'])
    failed = len(stocks) - successful
    
    print(f"\n{'='*70}")
    print(f"🎉 COLLECTION COMPLETE!")
    print(f"{'='*70}")
    print(f"📊 Summary:")
    print(f"   Total stocks: {len(stocks)}")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📈 Total price records: {total_price_records:,}")
    print(f"   💰 Total dividends: {total_dividends}")
    print(f"   📊 Total splits: {total_splits}")
    print(f"   ⏱️  Time elapsed: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"   ⚡ Avg per stock: {elapsed/len(stocks):.1f}s")
    print(f"{'='*70}\n")
    
    logger.info(f"\n{'='*70}")
    logger.info(f"COLLECTION COMPLETE")
    logger.info(f"{'='*70}")
    logger.info(f"Total stocks: {len(stocks)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Time elapsed: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    logger.info(f"{'='*70}")
    
    return all_stats


def main():
    parser = argparse.ArgumentParser(description='EODHD Data Collector')
    parser.add_argument('--stocks', required=True, help='Path to stock list file')
    parser.add_argument('--skip-price', action='store_true', help='Skip price data collection')
    parser.add_argument('--skip-actions', action='store_true', help='Skip corporate actions collection')
    parser.add_argument('--intraday-days', type=int, default=600, help='Days of intraday data (default: 600)')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between stocks in seconds (default: 0.5)')
    parser.add_argument('--limit', type=int, help='Limit number of stocks to process')
    
    # Advanced options (matching bulk_stock_collector.py)
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing (default: 50)')
    parser.add_argument('--resume', action='store_true', help='Resume from previous interrupted session')
    parser.add_argument('--retry-failed', action='store_true', help='Only retry previously failed stocks')
    parser.add_argument('--clean-start', action='store_true', help='Start fresh (ignore previous progress)')
    parser.add_argument('--update-mode', action='store_true', help='Incremental update: fetch only missing/outdated stocks')
    parser.add_argument('--max-age', type=int, default=1, help='Max age in days for update-mode (default: 1)')
    parser.add_argument('--force-update', action='store_true', help='Force full re-fetch of all stocks (ignore existing data)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    logger.info("="*70)
    logger.info("EODHD DATA COLLECTOR")
    logger.info("="*70)
    
    # Load stocks
    stocks = load_stocks(args.stocks)
    logger.info(f"Loaded {len(stocks)} stocks from {args.stocks}")
    
    # Limit if requested
    if args.limit:
        stocks = stocks[:args.limit]
        logger.info(f"Limited to {len(stocks)} stocks")
    
    # Run collection
    stats = collect_multiple_stocks(
        stocks,
        collect_price=not args.skip_price,
        collect_actions=not args.skip_actions,
        intraday_days=args.intraday_days,
        delay=args.delay
    )
    
    # Save stats to file
    stats_file = f"data/collection_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    os.makedirs('data', exist_ok=True)
    
    with open(stats_file, 'w') as f:
        for stat in stats:
            f.write(f"{stat}\n")
    
    logger.info(f"Stats saved to: {stats_file}")


if __name__ == "__main__":
    main()
