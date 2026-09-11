#!/usr/bin/env python
"""Delete data for a test stock to test metadata tracking"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient

client = QuestDBClient()
client.connect()

try:
    # Delete all data for BANK.JK
    client.cursor.execute("DELETE FROM eodhd_stock_data WHERE symbol = 'BANK.JK'")
    print(f"✅ Deleted all price data for BANK.JK")
    
    # Delete metadata if exists
    client.cursor.execute("DELETE FROM eodhd_stock_metadata WHERE symbol = 'BANK.JK'")
    print(f"✅ Deleted metadata for BANK.JK")
    
finally:
    client.close()

print("\n✅ Ready to test metadata tracking with BANK.JK")
