#!/usr/bin/env python
"""Check stock metadata table"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.questdb_client import QuestDBClient

client = QuestDBClient()
client.connect()

try:
    client.cursor.execute("""
        SELECT symbol, interval, data_end, total_records 
        FROM eodhd_stock_metadata 
        WHERE symbol IN ('BANK.JK', 'BRIS.JK', 'BTPS.JK')
        ORDER BY symbol, interval
    """)
    
    results = client.cursor.fetchall()
    
    print("\n" + "="*70)
    print("STOCK METADATA TABLE")
    print("="*70)
    print(f"{'Symbol':<12} {'Interval':<10} {'Data End':<20} {'Records':<10}")
    print("-"*70)
    
    for row in results:
        symbol, interval, data_end, total_records = row
        print(f"{symbol:<12} {interval:<10} {str(data_end):<20} {total_records:<10}")
    
    print("="*70)
    print(f"Total rows: {len(results)}\n")
    
finally:
    client.close()
