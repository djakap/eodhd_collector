"""
Optimized Main Orchestrator
Performance improvements:
1. Reduced intraday intervals (skip 5m, 15m for speed)
2. Connection pooling (reuse DB connections)
3. Batch processing with progress tracking
4. Skip NULL filtering for speed
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


def collect_single_stock_optimized(symbol: str, price_collector, action_collector,
                                   collect_price: bool = True, 
                                   collect_actions: bool = True,
                                   intraday_days: int = 120,
                                   skip_intraday: bool = False):
    """
    Optimized collection for a single stock (reuses collectors)
    
    Args:
        symbol: Stock symbol (e.g., 'BBCA.JK')
        price_collector: Reusable PriceCollector instance
        action_collector: Reusable ActionCollector instance
        collect_price: Whether to collect price data
        collect_actions: Whether to collect corporate actions
        intraday_days: Number of days for intraday data (reduced default)
        skip_intraday: Skip intraday data entirely for speed
    
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
            if skip_intraday:
                # EOD only (much faster)
                price_stats = {
                    'eod_records': price_collector.collect_eod_data(symbol),
                    'intraday_records': 0,
                    'total_records': 0
                }
                price_stats['total_records'] = price_stats['eod_records']
            else:
                price_stats = price_collector.collect_all_intervals(symbol, intraday_days=intraday_days)
            stats['price_stats'] = price_stats
        
        # Collect corporate actions
        if collect_actions:
            action_stats = action_collector.collect_all_actions(symbol)
            stats['action_stats'] = action_stats
        
        stats['success'] = True
        
    except Exception as e:
        stats['error'] = str(e)
        logger.error(f"Failed to collect {symbol}: {e}")
    
    return stats


def collect_multiple_stocks_optimized(stocks: list, collect_price: bool = True,
                                      collect_actions: bool = True,
                                      intraday_days: int = 120,
                                      skip_intraday: bool = False,
                                      delay: float = 0.2):
    """
    Optimized collection for multiple stocks
    
    Key optimizations:
    - Reuse DB connections (no reconnect per stock)
    - Reduced default intraday days (120 vs 600)
    - Option to skip intraday entirely
    - Reduced delay (0.2s vs 0.5s)
    
    Args:
        stocks: List of stock symbols
        collect_price: Whether to collect price data
        collect_actions: Whether to collect corporate actions
        intraday_days: Number of days for intraday data
        skip_intraday: Skip intraday data for maximum speed
        delay: Delay between stocks (seconds)
    
    Returns:
        List of collection stats
    """
    print(f"\n{'='*70}")
    print(f"🚀 OPTIMIZED EODHD DATA COLLECTION")
    print(f"{'='*70}")
    print(f"📊 Total stocks: {len(stocks)}")
    print(f"📈 Price data: {'Yes' if collect_price else 'No'}")
    print(f"💰 Corporate actions: {'Yes' if collect_actions else 'No'}")
    print(f"📅 Intraday days: {intraday_days if not skip_intraday else 'SKIPPED (EOD only)'}")
    print(f"⚡ Delay: {delay}s")
    print(f"{'='*70}\n")
    
    logger.info(f"Starting optimized collection for {len(stocks)} stocks")
    logger.info(f"Price data: {collect_price}, Actions: {collect_actions}")
    logger.info(f"Intraday days: {intraday_days}, Skip intraday: {skip_intraday}")
    
    # Create collectors ONCE and reuse (major optimization)
    price_collector = PriceCollector() if collect_price else None
    action_collector = ActionCollector() if collect_actions else None
    
    try:
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
            
            stock_stats = collect_single_stock_optimized(
                symbol,
                price_collector,
                action_collector,
                collect_price=collect_price,
                collect_actions=collect_actions,
                intraday_days=intraday_days,
                skip_intraday=skip_intraday
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
        
    finally:
        # Clean up connections
        if price_collector:
            price_collector.close()
        if action_collector:
            action_collector.close()


def main():
    parser = argparse.ArgumentParser(description='EODHD Data Collector (Optimized)')
    parser.add_argument('--stocks', required=True, help='Path to stock list file')
    parser.add_argument('--skip-price', action='store_true', help='Skip price data collection')
    parser.add_argument('--skip-actions', action='store_true', help='Skip corporate actions collection')
    parser.add_argument('--intraday-days', type=int, default=120, help='Days of intraday data (default: 120, reduced from 600)')
    parser.add_argument('--skip-intraday', action='store_true', help='Skip intraday data entirely (EOD only, MUCH faster)')
    parser.add_argument('--delay', type=float, default=0.2, help='Delay between stocks in seconds (default: 0.2)')
    parser.add_argument('--limit', type=int, help='Limit number of stocks to process')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    logger.info("="*70)
    logger.info("EODHD DATA COLLECTOR (OPTIMIZED)")
    logger.info("="*70)
    
    # Load stocks
    stocks = load_stocks(args.stocks)
    logger.info(f"Loaded {len(stocks)} stocks from {args.stocks}")
    
    # Limit if requested
    if args.limit:
        stocks = stocks[:args.limit]
        logger.info(f"Limited to {len(stocks)} stocks")
    
    # Run collection
    stats = collect_multiple_stocks_optimized(
        stocks,
        collect_price=not args.skip_price,
        collect_actions=not args.skip_actions,
        intraday_days=args.intraday_days,
        skip_intraday=args.skip_intraday,
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
