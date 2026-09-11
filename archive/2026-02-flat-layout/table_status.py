#!/usr/bin/env python
"""
EODHD Collector - Complete Table Status Report
Shows all tables and their implementation status
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient

client = QuestDBClient()
client.connect()

print("\n" + "="*80)
print("EODHD COLLECTOR - TABLE STATUS REPORT")
print("="*80)

tables_status = [
    {
        'table': 'eodhd_stock_data',
        'purpose': 'Price data (OHLC, volume)',
        'collector': 'collectors/price_collector.py',
        'status': '✅ IMPLEMENTED',
        'contains': 'EOD + Intraday (5m, 15m, 30m, 1h, 1d, 1w, 1m)',
        'update_mode': '✅ Yes',
        'script': 'main_ultrafast.py / run_update.sh'
    },
    {
        'table': 'eodhd_stock_metadata',
        'purpose': 'Update mode tracking',
        'collector': 'N/A (auto-updated by collectors)',
        'status': '✅ IMPLEMENTED',
        'contains': 'Data freshness tracking per stock/interval',
        'update_mode': '✅ Yes',
        'script': 'Auto-updated during collection'
    },
    {
        'table': 'eodhd_corporate_actions',
        'purpose': 'Dividends & splits',
        'collector': 'collectors/action_collector.py',
        'status': '✅ IMPLEMENTED',
        'contains': 'Dividend amounts, split ratios, dates',
        'update_mode': '❌ No',
        'script': 'main_ultrafast.py (--skip-actions to disable)'
    },
    {
        'table': 'eodhd_metadata',
        'purpose': 'Stock information',
        'collector': 'collectors/metadata_collector.py',
        'status': '✅ IMPLEMENTED',
        'contains': 'Name, exchange, currency, ISIN (944 stocks)',
        'update_mode': '❌ No (one-time)',
        'script': 'collect_metadata.py'
    },
    {
        'table': 'eodhd_fundamentals',
        'purpose': 'Financial data',
        'collector': 'NOT IMPLEMENTED',
        'status': '❌ NOT AVAILABLE',
        'contains': 'PE ratio, revenue, margins (403 Forbidden)',
        'update_mode': 'N/A',
        'script': 'N/A - Requires subscription upgrade'
    },
    {
        'table': 'eodhd_calendar_events',
        'purpose': 'Earnings, IPOs, events',
        'collector': 'NOT IMPLEMENTED',
        'status': '❌ NOT AVAILABLE',
        'contains': 'Earnings dates, IPO dates (403 Forbidden)',
        'update_mode': 'N/A',
        'script': 'N/A - Requires subscription upgrade'
    }
]

# Get actual record counts
try:
    client.cursor.execute("SELECT COUNT(*) FROM eodhd_stock_data")
    price_count = client.cursor.fetchone()[0]
except:
    price_count = 0

try:
    client.cursor.execute("SELECT COUNT(*) FROM eodhd_stock_metadata")
    metadata_tracking_count = client.cursor.fetchone()[0]
except:
    metadata_tracking_count = 0

try:
    client.cursor.execute("SELECT COUNT(*) FROM eodhd_corporate_actions")
    actions_count = client.cursor.fetchone()[0]
except:
    actions_count = 0

try:
    client.cursor.execute("SELECT COUNT(*) FROM eodhd_metadata")
    metadata_count = client.cursor.fetchone()[0]
except:
    metadata_count = 0

client.close()

print("\n📊 TABLE STATUS:\n")
print(f"{'#':<3} {'Table Name':<30} {'Status':<20} {'Records':<15}")
print("-"*80)

print(f"{'1':<3} {'eodhd_stock_data':<30} {'✅ IMPLEMENTED':<20} {f'{price_count:,}':<15}")
print(f"{'2':<3} {'eodhd_stock_metadata':<30} {'✅ IMPLEMENTED':<20} {f'{metadata_tracking_count:,}':<15}")
print(f"{'3':<3} {'eodhd_corporate_actions':<30} {'✅ IMPLEMENTED':<20} {f'{actions_count:,}':<15}")
print(f"{'4':<3} {'eodhd_metadata':<30} {'✅ IMPLEMENTED':<20} {f'{metadata_count:,}':<15}")
print(f"{'5':<3} {'eodhd_fundamentals':<30} {'❌ NOT AVAILABLE':<20} {'N/A':<15}")
print(f"{'6':<3} {'eodhd_calendar_events':<30} {'❌ NOT AVAILABLE':<20} {'N/A':<15}")

print("\n" + "="*80)
print("DETAILED STATUS:")
print("="*80)

for i, table in enumerate(tables_status, 1):
    print(f"\n{i}. {table['table'].upper()}")
    print(f"   Purpose: {table['purpose']}")
    print(f"   Status: {table['status']}")
    print(f"   Collector: {table['collector']}")
    print(f"   Contains: {table['contains']}")
    print(f"   Update Mode: {table['update_mode']}")
    print(f"   Script: {table['script']}")

print("\n" + "="*80)
print("SUMMARY:")
print("="*80)
print("✅ Implemented & Working: 4 tables")
print("   - eodhd_stock_data (price data)")
print("   - eodhd_stock_metadata (update tracking)")
print("   - eodhd_corporate_actions (dividends, splits)")
print("   - eodhd_metadata (stock info)")
print()
print("❌ Not Available (Subscription): 2 tables")
print("   - eodhd_fundamentals (requires upgrade)")
print("   - eodhd_calendar_events (requires upgrade)")
print()
print("📊 Total Records Collected:")
print(f"   - Price data: {price_count:,}")
print(f"   - Corporate actions: {actions_count:,}")
print(f"   - Stock metadata: {metadata_count:,}")
print(f"   - Update tracking: {metadata_tracking_count:,}")
print("="*80 + "\n")
