#!/usr/bin/env python
"""Test intraday data availability and limits"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.eodhd_client import EODHDClient
from datetime import datetime, timedelta

client = EODHDClient()
test_symbol = "BBCA.JK"

print("\n" + "="*70)
print("INTRADAY DATA AVAILABILITY TEST")
print("="*70)
print(f"Testing with: {test_symbol}\n")

# Test different date ranges
test_ranges = [
    ("7 days", 7),
    ("30 days", 30),
    ("120 days", 120),
    ("365 days", 365),
    ("600 days", 600),
    ("730 days (2 years)", 730),
]

for label, days in test_ranges:
    print(f"\n{label} ({days} days):")
    print("-" * 70)
    
    from_date = datetime.now() - timedelta(days=days)
    to_date = datetime.now()
    
    try:
        data = client.get_intraday_data(
            test_symbol, 
            interval='1h',
            from_timestamp=int(from_date.timestamp()),
            to_timestamp=int(to_date.timestamp())
        )
        
        if data:
            print(f"✅ AVAILABLE - {len(data)} records")
            if len(data) > 0:
                first = data[0]
                last = data[-1]
                print(f"   First: {first.get('datetime', 'N/A')}")
                print(f"   Last: {last.get('datetime', 'N/A')}")
        else:
            print("❌ NO DATA or ERROR")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print("Check which date ranges returned data successfully.")
print("="*70 + "\n")
