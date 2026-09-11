#!/usr/bin/env python
"""Check if EODHD 'close' is adjusted or raw"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.eodhd_client import EODHDClient
from datetime import datetime, timedelta

client = EODHDClient()

# Test with a stock that has had splits/dividends
test_symbol = "BBCA.JK"  # Bank Central Asia - likely has dividends

print("\n" + "="*70)
print("EODHD CLOSE vs ADJUSTED_CLOSE ANALYSIS")
print("="*70)
print(f"Testing with: {test_symbol}\n")

# Get recent EOD data
data = client.get_eod_data(test_symbol, period='d')

if data and len(data) > 0:
    print("Sample of recent data:")
    print("-" * 70)
    print(f"{'Date':<12} {'Close':<12} {'Adjusted':<12} {'Difference':<12}")
    print("-" * 70)
    
    # Show last 10 records
    for record in data[-10:]:
        date = record.get('date', 'N/A')
        close = record.get('close', 0)
        adjusted = record.get('adjusted_close', 0)
        diff = abs(close - adjusted) if close and adjusted else 0
        
        print(f"{date:<12} {close:<12.2f} {adjusted:<12.2f} {diff:<12.4f}")
    
    print("\n" + "="*70)
    print("ANALYSIS:")
    print("="*70)
    
    # Check if close and adjusted_close are different
    has_difference = False
    for record in data:
        close = record.get('close', 0)
        adjusted = record.get('adjusted_close', 0)
        if close and adjusted and abs(close - adjusted) > 0.01:
            has_difference = True
            break
    
    if has_difference:
        print("✅ 'close' and 'adjusted_close' are DIFFERENT")
        print("   → 'close' = RAW/UNADJUSTED price")
        print("   → 'adjusted_close' = ADJUSTED for splits/dividends")
    else:
        print("⚠️  'close' and 'adjusted_close' are the SAME")
        print("   → Either no corporate actions, or 'close' is already adjusted")
    
    print("\n" + "="*70)
    print("EODHD API BEHAVIOR:")
    print("="*70)
    print("According to EODHD documentation:")
    print("  - 'close': Closing price (typically UNADJUSTED)")
    print("  - 'adjusted_close': Adjusted for splits and dividends")
    print("\nFor technical analysis and backtesting:")
    print("  → Use 'adjusted_close' for accurate historical comparisons")
    print("  → Use 'close' for current/real-time prices")
    print("="*70 + "\n")

else:
    print("❌ No data received")
