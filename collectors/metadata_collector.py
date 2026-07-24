"""
Stock Metadata Collector
Collects stock metadata (name, exchange, currency, ISIN) from EODHD API
"""

import logging
from datetime import datetime
from typing import List, Optional

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from config.db_config import TABLE_METADATA

logger = logging.getLogger(__name__)


class MetadataCollector:
    """Collector for stock metadata"""
    
    def __init__(self):
        self.api_client = EODHDClient()
        self.db_client = QuestDBClient()
        self.db_client.connect()
    
    def close(self):
        """Close database connection and HTTP client"""
        try:
            self.db_client.close()
        finally:
            # Close the httpx client too — otherwise its connection pool leaks
            self.api_client.close()
    
    def collect_metadata(self, symbol: str) -> Optional[dict]:
        """
        Collect metadata for a single stock
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
        
        Returns:
            Dict with collection stats or None
        """
        try:
            logger.info(f"Collecting metadata for {symbol}")
            
            # Fetch metadata from API
            metadata = self.api_client.get_stock_metadata(symbol)
            
            if not metadata:
                logger.warning(f"No metadata found for {symbol}")
                return None
            
            # Insert into database
            self._insert_metadata(symbol, metadata)
            
            logger.info(f"✅ Collected metadata for {symbol}")
            
            return {
                'symbol': symbol,
                'name': metadata.get('Name'),
                'exchange': metadata.get('Exchange'),
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Failed to collect metadata for {symbol}: {e}")
            return {
                'symbol': symbol,
                'success': False,
                'error': str(e)
            }
    
    def collect_exchange_metadata(self, exchange: str = 'JK') -> dict:
        """
        Collect metadata for all stocks in an exchange
        
        Args:
            exchange: Exchange code (default: 'JK')
        
        Returns:
            Dict with collection stats
        """
        try:
            logger.info(f"Collecting metadata for exchange {exchange}")
            
            # Fetch all symbols from exchange
            symbols_data = self.api_client.get_exchange_symbols(exchange)
            
            if not symbols_data:
                logger.error(f"No symbols found for exchange {exchange}")
                return {'success': False, 'total': 0}
            
            logger.info(f"Found {len(symbols_data)} symbols on {exchange} exchange")
            
            # Insert all metadata
            inserted = 0
            for symbol_info in symbols_data:
                try:
                    symbol = f"{symbol_info['Code']}.{exchange}"
                    self._insert_metadata(symbol, symbol_info)
                    inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert {symbol_info.get('Code')}: {e}")
            
            logger.info(f"✅ Inserted {inserted}/{len(symbols_data)} metadata records")
            
            return {
                'success': True,
                'total': len(symbols_data),
                'inserted': inserted,
                'exchange': exchange
            }
            
        except Exception as e:
            logger.error(f"Failed to collect exchange metadata: {e}")
            return {'success': False, 'error': str(e)}
    
    def _insert_metadata(self, symbol: str, metadata: dict):
        """
        Upsert one symbol's metadata.

        eodhd_metadata is keyed by symbol and has DEDUP disabled (its designated
        timestamp is updated_at, the insert time, which cannot be part of a dedup
        key). A plain INSERT therefore appends a new row on every run — re-running
        this collector once would have duplicated all 953 symbols. So it checks for
        an existing row and UPDATEs it, inserting only when the symbol is new, the
        same pattern QuestDBClient.upsert_stock_metadata already uses.

        LEGACY: this pulls company metadata from EODHD, which is retired as of the
        2026-07-24 cutover to yfinance (company profiles now come from yf_profile).
        It is kept idempotent rather than removed so a stray manual run can do no
        harm.
        """
        now = datetime.now()
        exchange = metadata.get('Exchange', 'JK')
        name = metadata.get('Name', '')
        sector = metadata.get('Sector', '')
        industry = metadata.get('Industry', '')
        currency = metadata.get('Currency', 'IDR')

        try:
            self.db_client.cursor.execute(
                f"SELECT symbol FROM {TABLE_METADATA} WHERE symbol = %s LIMIT 1",
                (symbol,))
            exists = self.db_client.cursor.fetchone() is not None

            if exists:
                # updated_at is the designated timestamp and QuestDB refuses to
                # update it in place, so freshness is tracked via last_price_update
                # instead. updated_at keeps its original value on an existing row.
                self.db_client.cursor.execute(f"""
                    UPDATE {TABLE_METADATA}
                    SET exchange = %s, name = %s, sector = %s, industry = %s,
                        currency = %s, last_price_update = %s, is_active = %s
                    WHERE symbol = %s
                """, (exchange, name, sector, industry, currency, now, True,
                      symbol))
                logger.debug(f"Updated metadata for {symbol}")
            else:
                self.db_client.cursor.execute(f"""
                    INSERT INTO {TABLE_METADATA}
                    (symbol, exchange, name, sector, industry, currency,
                     last_price_update, has_dividends, is_active, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (symbol, exchange, name, sector, industry, currency,
                      now, False, True, now, now))
                logger.debug(f"Inserted metadata for {symbol}")
        except Exception as e:
            logger.error(f"Failed to upsert metadata for {symbol}: {e}")
            raise
