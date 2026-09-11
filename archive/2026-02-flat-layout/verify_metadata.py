#!/usr/bin/env python
"""Verify metadata collection"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient

client = QuestDBClient()
client.connect()

try:
    # Count total metadata records
    client.cursor.execute("SELECT COUNT(*) FROM eodhd_metadata")
    total = client.cursor.fetchone()[0]
    
    # Sample some records
    client.cursor.execute("""
        SELECT symbol, name, exchange, currency 
        FROM eodhd_metadata 
        LIMIT 10
    """)
    samples = client.cursor.fetchall()
    
    print("\n" + "="*70)
    print("METADATA COLLECTION VERIFICATION")
    print("="*70)
    print(f"\nTotal metadata records: {total}")
    print("\nSample records:")
    print("-"*70)
    print(f"{'Symbol':<12} {'Name':<30} {'Exchange':<10} {'Currency':<8}")
    print("-"*70)
    
    for symbol, name, exchange, currency in samples:
        print(f"{symbol:<12} {name[:28]:<30} {exchange:<10} {currency:<8}")
    
    print("="*70 + "\n")
    
finally:
    client.close()
