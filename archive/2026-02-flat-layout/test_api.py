"""
Test EODHD API Client
Quick test to verify API connection and data fetching
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.eodhd_client import EODHDClient
from utils.logger import setup_logging
from utils.data_filter import filter_and_validate
from datetime import datetime, timedelta
import logging

# Setup logging
setup_logging(log_level='INFO')
logger = logging.getLogger(__name__)

def test_api_connection():
    """Test basic API connection"""
    print("="*70)
    print("Testing EODHD API Connection")
    print("="*70)
    
    client = EODHDClient()
    
    # Test with BBCA.JK (Bank Central Asia)
    test_symbol = 'BBCA.JK'
    
    print(f"\n1. Testing EOD data for {test_symbol}...")
    eod_data = client.get_eod_data(test_symbol, period='d')
    if eod_data:
        print(f"   ✅ Success! Got {len(eod_data)} daily records")
        print(f"   Latest: {eod_data[-1]}")
    else:
        print(f"   ❌ Failed to fetch EOD data")
    
    print(f"\n2. Testing Intraday data for {test_symbol} (5m)...")
    # Get last 7 days
    to_ts = int(datetime.now().timestamp())
    from_ts = int((datetime.now() - timedelta(days=7)).timestamp())
    
    intraday_data = client.get_intraday_data(test_symbol, interval='5m', 
                                             from_timestamp=from_ts,
                                             to_timestamp=to_ts)
    if intraday_data:
        print(f"   ✅ Success! Got {len(intraday_data)} 5m records")
        
        # Filter NULL values
        valid_data = filter_and_validate(intraday_data)
        print(f"   Valid records: {len(valid_data)} ({len(valid_data)/len(intraday_data)*100:.1f}%)")
        
        if valid_data:
            print(f"   Latest valid: {valid_data[-1]}")
    else:
        print(f"   ❌ Failed to fetch intraday data")
    
    print(f"\n3. Testing Fundamentals for {test_symbol}...")
    fundamentals = client.get_fundamentals(test_symbol)
    if fundamentals:
        print(f"   ✅ Success! Got fundamental data")
        if 'General' in fundamentals:
            print(f"   Company: {fundamentals['General'].get('Name', 'N/A')}")
            print(f"   Sector: {fundamentals['General'].get('Sector', 'N/A')}")
    else:
        print(f"   ❌ Failed to fetch fundamentals")
    
    print(f"\n4. Testing Dividends for {test_symbol}...")
    dividends = client.get_dividends(test_symbol)
    if dividends:
        print(f"   ✅ Success! Got {len(dividends)} dividend records")
        if dividends:
            print(f"   Latest: {dividends[-1]}")
    else:
        print(f"   ⚠️  No dividend data (or failed)")
    
    print("\n" + "="*70)
    print("✅ API Connection Test Complete!")
    print("="*70)

if __name__ == "__main__":
    test_api_connection()
