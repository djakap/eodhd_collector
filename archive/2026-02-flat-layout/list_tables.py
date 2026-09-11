#!/usr/bin/env python
"""List all tables in QuestDB"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient

client = QuestDBClient()
client.connect()

try:
    # Query to list all tables
    client.cursor.execute("SHOW TABLES")
    
    tables = client.cursor.fetchall()
    
    print("\n" + "="*70)
    print("QUESTDB TABLES")
    print("="*70)
    
    if tables:
        print(f"\nFound {len(tables)} tables:\n")
        for i, (table_name,) in enumerate(tables, 1):
            print(f"{i:2}. {table_name}")
    else:
        print("\nNo tables found.")
    
    print("\n" + "="*70 + "\n")
    
finally:
    client.close()
