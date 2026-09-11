#!/usr/bin/env python
"""Check EODHD API subscription access"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.eodhd_client import EODHDClient
import json

client = EODHDClient()

# Test stock for checking
test_symbol = "BBCA.JK"

print("\n" + "="*70)
print("EODHD API SUBSCRIPTION CHECK")
print("="*70)
print(f"Testing with: {test_symbol}\n")

# 1. Fundamentals
print("1. FUNDAMENTALS DATA")
print("-" * 70)
fundamentals = client.get_fundamentals(test_symbol)
if fundamentals:
    print("✅ AVAILABLE")
    print(f"   Sample keys: {list(fundamentals.keys())[:5]}")
else:
    print("❌ NOT AVAILABLE or NO DATA")

# 2. Dividends (already working)
print("\n2. DIVIDENDS DATA")
print("-" * 70)
dividends = client.get_dividends(test_symbol)
if dividends:
    print(f"✅ AVAILABLE ({len(dividends)} records)")
else:
    print("❌ NOT AVAILABLE or NO DATA")

# 3. Splits (already working)
print("\n3. SPLITS DATA")
print("-" * 70)
splits = client.get_splits(test_symbol)
if splits:
    print(f"✅ AVAILABLE ({len(splits)} records)")
else:
    print("❌ NOT AVAILABLE or NO DATA")

# 4. Calendar/Earnings (need to check if endpoint exists)
print("\n4. CALENDAR/EARNINGS DATA")
print("-" * 70)
try:
    # Check if earnings calendar endpoint is available
    from config.eodhd_config import EARNINGS_CALENDAR_ENDPOINT
    print(f"   Endpoint configured: {EARNINGS_CALENDAR_ENDPOINT}")
    print("   ⚠️  Need to implement collector to test")
except:
    print("   ❌ Endpoint not configured")

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print("✅ = Available in your subscription")
print("❌ = Not available or not implemented")
print("⚠️  = Unknown (need to test)")
print("="*70 + "\n")
