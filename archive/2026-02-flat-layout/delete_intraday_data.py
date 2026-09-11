#!/usr/bin/env python
"""Delete all intraday data to restart with new NULL-preserving logic"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient
from config.db_config import TABLE_STOCK_DATA

client = QuestDBClient()
client.connect()

try:
    print("\n" + "="*70)
    print("DELETING INTRADAY DATA")
    print("="*70)
    
    # Count existing intraday records
    client.cursor.execute(f"""
        SELECT COUNT(*) FROM {TABLE_STOCK_DATA}
        WHERE source = 'intraday'
    """)
    count = client.cursor.fetchone()[0]
    
    print(f"\nExisting intraday records: {count:,}")
    
    if count > 0:
        print("\nDeleting intraday data...")
        
        # Delete all intraday records
        client.cursor.execute(f"""
            DELETE FROM {TABLE_STOCK_DATA}
            WHERE source = 'intraday'
        """)
        
        print(f"✅ Deleted {count:,} intraday records")
    else:
        print("No intraday data to delete")
    
    # Also delete intraday metadata
    print("\nDeleting intraday metadata...")
    client.cursor.execute(f"""
        DELETE FROM eodhd_stock_metadata
        WHERE interval IN ('5m', '15m', '30m', '1h')
    """)
    print("✅ Deleted intraday metadata")
    
    print("\n" + "="*70)
    print("READY FOR FRESH COLLECTION")
    print("="*70)
    print("You can now run:")
    print("python main_ultrafast.py --stocks config/syariah_stocks.txt --intraday-days 600 --skip-actions")
    print("="*70 + "\n")
    
finally:
    client.close()
