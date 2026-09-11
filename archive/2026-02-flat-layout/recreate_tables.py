#!/usr/bin/env python
"""
Drop and recreate eodhd_stock_data table
This will remove all existing data and create a fresh table
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient
from config.db_config import TABLE_STOCK_DATA

client = QuestDBClient()
client.connect()

try:
    print("\n" + "="*70)
    print("DROPPING AND RECREATING TABLE")
    print("="*70)
    
    # Count current records
    print("\nCurrent data:")
    client.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_STOCK_DATA} WHERE source = 'eod'")
    eod_count = client.cursor.fetchone()[0]
    
    client.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_STOCK_DATA} WHERE source = 'intraday'")
    intraday_count = client.cursor.fetchone()[0]
    
    print(f"  EOD records: {eod_count:,}")
    print(f"  Intraday records: {intraday_count:,}")
    print(f"  Total: {eod_count + intraday_count:,}")
    
    # Drop table
    print(f"\n⚠️  Dropping table {TABLE_STOCK_DATA}...")
    client.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_STOCK_DATA}")
    print(f"✅ Table dropped")
    
    # Recreate table
    print(f"\n📋 Recreating table {TABLE_STOCK_DATA}...")
    client.cursor.execute(f"""
        CREATE TABLE {TABLE_STOCK_DATA} (
            symbol SYMBOL,              -- Stock symbol (e.g., 'BBCA.JK')
            interval SYMBOL,            -- '5m', '15m', '30m', '1h', '1d', '1w', '1M'
            timestamp TIMESTAMP,        -- Bar timestamp (designated timestamp)
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adjusted_close DOUBLE,      -- NULL for intraday, populated for EOD
            volume LONG,
            gmtoffset INT,              -- GMT offset (from intraday API), NULL for EOD
            source SYMBOL,              -- 'eod' or 'intraday'
            created_at TIMESTAMP        -- When record was inserted
        ) timestamp(timestamp) PARTITION BY DAY WAL;
    """)
    print(f"✅ Table recreated")
    
    # Also drop metadata table
    print(f"\n📋 Dropping metadata table...")
    client.cursor.execute("DROP TABLE IF EXISTS eodhd_stock_metadata")
    print(f"✅ Metadata table dropped")
    
    print(f"\n📋 Recreating metadata table...")
    client.cursor.execute("""
        CREATE TABLE IF NOT EXISTS eodhd_stock_metadata (
            symbol SYMBOL,
            interval SYMBOL,
            last_updated TIMESTAMP,
            total_records LONG,
            data_start TIMESTAMP,
            data_end TIMESTAMP,
            created_at TIMESTAMP
        ) timestamp(last_updated) PARTITION BY DAY WAL;
    """)
    print(f"✅ Metadata table recreated")
    
    print("\n" + "="*70)
    print("✅ TABLES RECREATED SUCCESSFULLY")
    print("="*70)
    print("\nNext steps:")
    print("1. Collect EOD data:")
    print("   python main_ultrafast.py --stocks config/syariah_stocks.txt --skip-intraday")
    print("\n2. Collect intraday data (with new NULL logic):")
    print("   python main_ultrafast.py --stocks config/syariah_stocks.txt --intraday-days 600 --skip-actions")
    print("="*70 + "\n")
    
finally:
    client.close()
