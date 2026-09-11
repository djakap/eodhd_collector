#!/usr/bin/env python
"""Test Calendar/Earnings and Stock Metadata endpoints"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.eodhd_client import EODHDClient
import requests
from datetime import datetime, timedelta

client = EODHDClient()
test_symbol = "BBCA.JK"

print("\n" + "="*70)
print("TESTING CALENDAR & METADATA ENDPOINTS")
print("="*70)

# Test 1: Stock Metadata/Exchange Details
print("\n1. STOCK METADATA (Exchange Symbol Details)")
print("-" * 70)
try:
    url = f"https://eodhistoricaldata.com/api/exchange-symbol-list/JK"
    params = {'api_token': client.api_key, 'fmt': 'json'}
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ AVAILABLE")
        print(f"   Total symbols: {len(data)}")
        # Find BBCA
        bbca = [s for s in data if s.get('Code') == 'BBCA']
        if bbca:
            print(f"   Sample (BBCA): {bbca[0]}")
    elif response.status_code == 403:
        print("❌ NOT AVAILABLE (403 Forbidden - Not in subscription)")
    else:
        print(f"❌ Error: {response.status_code}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 2: Earnings Calendar
print("\n2. EARNINGS CALENDAR")
print("-" * 70)
try:
    # Try to get earnings calendar for next 30 days
    from_date = datetime.now().strftime('%Y-%m-%d')
    to_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    
    url = f"https://eodhistoricaldata.com/api/calendar/earnings"
    params = {
        'api_token': client.api_key,
        'fmt': 'json',
        'from': from_date,
        'to': to_date,
        'symbols': test_symbol
    }
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ AVAILABLE")
        print(f"   Response type: {type(data)}")
        if isinstance(data, dict):
            print(f"   Keys: {list(data.keys())[:5]}")
        elif isinstance(data, list):
            print(f"   Records: {len(data)}")
    elif response.status_code == 403:
        print("❌ NOT AVAILABLE (403 Forbidden - Not in subscription)")
    else:
        print(f"❌ Error: {response.status_code}")
        print(f"   Response: {response.text[:200]}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 3: IPO Calendar
print("\n3. IPO CALENDAR")
print("-" * 70)
try:
    url = f"https://eodhistoricaldata.com/api/calendar/ipos"
    params = {
        'api_token': client.api_key,
        'fmt': 'json',
        'from': from_date,
        'to': to_date
    }
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ AVAILABLE")
        print(f"   Response type: {type(data)}")
    elif response.status_code == 403:
        print("❌ NOT AVAILABLE (403 Forbidden - Not in subscription)")
    else:
        print(f"❌ Error: {response.status_code}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 4: Stock Details (alternative metadata)
print("\n4. STOCK DETAILS (Alternative Metadata)")
print("-" * 70)
try:
    url = f"https://eodhistoricaldata.com/api/search/{test_symbol}"
    params = {'api_token': client.api_key, 'fmt': 'json'}
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ AVAILABLE")
        print(f"   Results: {len(data) if isinstance(data, list) else 1}")
        if data:
            print(f"   Sample: {data[0] if isinstance(data, list) else data}")
    elif response.status_code == 403:
        print("❌ NOT AVAILABLE (403 Forbidden - Not in subscription)")
    else:
        print(f"❌ Error: {response.status_code}")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70 + "\n")
