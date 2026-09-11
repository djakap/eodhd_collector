#!/usr/bin/env python
"""
Alternative approach: Create a new table without intraday data
Since QuestDB doesn't support DELETE FROM, we'll:
1. Create a temporary table with only EOD data
2. Drop original table
3. Rename temp table
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
    print("REMOVING INTRADAY DATA (QuestDB Method)")
    print("="*70)
    
    # Count records
    client.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_STOCK_DATA} WHERE source = 'intraday'")
    intraday_count = client.cursor.fetchone()[0]
    
    client.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_STOCK_DATA} WHERE source = 'eod'")
    eod_count = client.cursor.fetchone()[0]
    
    print(f"\nCurrent data:")
    print(f"  EOD records: {eod_count:,}")
    print(f"  Intraday records: {intraday_count:,}")
    print(f"  Total: {eod_count + intraday_count:,}")
    
    if intraday_count == 0:
        print("\n✅ No intraday data to remove")
    else:
        print(f"\n⚠️  Found {intraday_count:,} intraday records")
        print("\nOptions:")
        print("1. Keep old intraday data (will have wrong NULL handling)")
        print("2. Manually drop table and recreate (requires restart)")
        print("\nRecommendation: Just proceed with collection")
        print("  - Duplicate detection will skip existing records")
        print("  - New records will use correct NULL logic")
        print("  - Old records will remain with old logic")
        print("\nOr manually drop table:")
        print("  1. Stop QuestDB")
        print("  2. Delete table files")
        print("  3. Restart QuestDB")
        print("  4. Run: python db/create_tables.py")
    
    print("\n" + "="*70 + "\n")
    
finally:
    client.close()
