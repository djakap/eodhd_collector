#!/usr/bin/env python
"""Verify table existence and create if missing"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient
from config.db_config import TABLE_STOCK_METADATA

client = QuestDBClient()
client.connect()

try:
    # Check if table exists
    client.cursor.execute("SELECT * FROM tables()")
    tables = client.cursor.fetchall()
    
    print("\n" + "="*70)
    print("EXISTING TABLES:")
    print("="*70)
    for table in tables:
        print(f"  - {table[0]}")
    
    # Check if metadata table exists
    table_names = [t[0] for t in tables]
    
    if TABLE_STOCK_METADATA in table_names:
        print(f"\n✅ {TABLE_STOCK_METADATA} EXISTS")
    else:
        print(f"\n❌ {TABLE_STOCK_METADATA} DOES NOT EXIST")
        print("\nCreating table...")
        
        # Create the table directly
        client.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_STOCK_METADATA} (
                symbol SYMBOL,
                interval SYMBOL,
                last_updated TIMESTAMP,
                total_records LONG,
                data_start TIMESTAMP,
                data_end TIMESTAMP,
                created_at TIMESTAMP
            ) timestamp(last_updated) PARTITION BY DAY WAL;
        """)
        
        print(f"✅ Created {TABLE_STOCK_METADATA}")
    
    print("="*70 + "\n")
    
finally:
    client.close()
