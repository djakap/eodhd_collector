"""
QuestDB Client
Handles database operations with ILP (InfluxDB Line Protocol) support for 10-100x faster inserts
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch
from typing import List, Dict, Optional, Set, Tuple
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import logging
import time
import threading

try:
    from questdb.ingress import Sender, Protocol, IngressError, TimestampNanos
    HAS_ILP = True
except ImportError:
    HAS_ILP = False
    TimestampNanos = None
    logger = logging.getLogger(__name__)  # Initialize logger here if import fails

from config.db_config import (
    PG_CONNECTION_STRING,
    TABLE_CALENDAR_EVENTS,
    TABLE_METADATA,
    TABLE_STOCK_METADATA,
    BATCH_INSERT_SIZE,
    QUESTDB_HOST,
    QUESTDB_INFLUX_PORT
)
from utils.bar_rules import describe_rejection


def _as_utc(value):
    """
    Normalise a created_at value to a TZ-AWARE UTC datetime.

    Every caller fills this with datetime.now(), which returns the HOST's local
    time — WIB here, UTC inside the container. Sent as-is, the same run would
    stamp provenance seven hours apart depending on where it executed, which is
    the exact bug just removed from the market timestamp. A naive value is
    therefore read as host-local and converted; a tz-aware one is trusted.

    The result stays tz-aware on purpose. Handing the ILP client a naive datetime
    makes it apply its OWN local-to-UTC conversion, so a value already converted
    here lands seven hours early — measured: 03:00 UTC became 20:00 the previous
    day. Keeping the offset attached leaves the client nothing to guess.

    None becomes the current instant rather than staying NULL: a row always has a
    moment when it was written, and leaving the column empty is what made
    13,167,158 rows untraceable.
    """
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.astimezone(timezone.utc)
        return value.astimezone(timezone.utc)
    return value

logger = logging.getLogger(__name__)

# Corporate actions predate nothing on IDX before the mid-90s (oldest real action
# on record: 1995-06-02), so the whole of 1970 can be treated as invalid. That
# catches epoch zero and anything landing near it.
_MIN_ACTION_YEAR = 1971


def _coerce_action_date(value):
    """
    Normalise a corporate-action date, returning (date, None) or (None, reason).

    Accepts date, datetime and 'YYYY-MM-DD' strings, because callers legitimately
    differ: the EODHD collectors hand over `date` objects while an API payload or a
    future collector may pass the raw string.

    The two rejection reasons are kept distinct on purpose. 'implausible' means bad
    data from upstream; 'unparseable' means a caller passed the wrong type, which is
    a code bug. An earlier version of this guard used getattr(value, 'year', 0) and
    so scored a valid '2026-05-13' string as year 0 — silently discarding good data
    while logging it as an epoch-zero data problem.
    """
    if value is None:
        return None, 'implausible'

    if isinstance(value, str):
        # An empty string is how APIs spell "no value"; that is absent data, not a
        # caller passing the wrong type, and must not raise a code-bug alarm.
        if not value.strip():
            return None, 'implausible'
        try:
            value = datetime.strptime(value[:10], '%Y-%m-%d')
        except (ValueError, TypeError):
            return None, 'unparseable'

    if isinstance(value, datetime):
        value = value.date()

    if not isinstance(value, date):
        return None, 'unparseable'

    # A dividend may be declared for next year, but not beyond that.
    if value.year < _MIN_ACTION_YEAR or value.year > datetime.now().year + 1:
        return None, 'implausible'

    return value, None


class QuestDBClient:
    """Client for QuestDB operations with ILP support for maximum performance"""
    
    # Class-level connection pool (shared across all instances)
    _connection_pool = None
    _pool_lock = threading.Lock()
    _use_ilp = HAS_ILP  # Class variable to control ILP usage
    _ilp_lock = threading.Lock()  # Lock for ILP operations (thread-safe)
    
    def __init__(self, use_ilp: bool = True):
        self.connection_string = PG_CONNECTION_STRING
        self.conn = None
        self.cursor = None
        self.use_ilp = use_ilp and QuestDBClient._use_ilp  # Instance setting
        self.ilp_host = QUESTDB_HOST
        self.ilp_port = QUESTDB_INFLUX_PORT
        
        # Initialize connection pool (singleton pattern)
        if QuestDBClient._connection_pool is None:
            with QuestDBClient._pool_lock:
                if QuestDBClient._connection_pool is None:
                    try:
                        QuestDBClient._connection_pool = pool.ThreadedConnectionPool(
                            minconn=1,
                            maxconn=20,  # Support up to 20 parallel workers
                            **self._parse_connection_string(PG_CONNECTION_STRING)
                        )
                        logger.info("Created QuestDB connection pool (1-20 connections)")
                        
                        # Test ILP connection if available
                        if HAS_ILP and use_ilp:
                            try:
                                with Sender(Protocol.Tcp, self.ilp_host, self.ilp_port) as sender:
                                    pass  # Just test connection
                                logger.info(f"✅ QuestDB ILP available at {self.ilp_host}:{self.ilp_port} (10-100x faster inserts)")
                            except Exception as e:
                                logger.warning(f"ILP connection test failed, will use SQL: {e}")
                                QuestDBClient._use_ilp = False
                                self.use_ilp = False
                        
                    except Exception as e:
                        logger.warning(f"Failed to create connection pool: {e}, using single connection")
                        QuestDBClient._connection_pool = None
    
    def _parse_connection_string(self, conn_str: str) -> dict:
        """Parse PostgreSQL connection string to dict"""
        # postgresql://user:password@host:port/database
        import re
        match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', conn_str)
        if match:
            return {
                'user': match.group(1),
                'password': match.group(2),
                'host': match.group(3),
                'port': int(match.group(4)),
                'database': match.group(5)
            }
        return {}
    
    def connect(self, retries=3, backoff=1.0):
        """Establish database connection (from pool if available) with retry logic"""
        last_error = None
        for attempt in range(retries):
            try:
                if QuestDBClient._connection_pool is not None:
                    # Get connection from pool
                    self.conn = QuestDBClient._connection_pool.getconn()
                    self.conn.autocommit = True
                else:
                    # Fallback to single connection
                    self.conn = psycopg2.connect(self.connection_string)
                    self.conn.autocommit = True
                
                self.cursor = self.conn.cursor()
                logger.debug("Connected to QuestDB")
                return  # Success
                
            except Exception as e:
                last_error = e
                # If connection refused, the pool may have stale connections
                # or QuestDB may have restarted — reset the pool so next attempt
                # creates fresh connections
                if 'Connection refused' in str(e) or 'server closed the connection' in str(e):
                    self._reset_pool()
                if attempt < retries - 1:
                    wait_time = backoff * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Connection attempt {attempt + 1}/{retries} failed, retrying in {wait_time:.1f}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect to QuestDB after {retries} attempts: {e}")
                    raise last_error

    @classmethod
    def close_pool(cls):
        """Close all pooled connections and drop the pool.

        Call this once at the very end of a run (e.g. after a flow finishes).
        The pool is class-level/shared and otherwise keeps up to maxconn
        connections open for the lifetime of the process — which leaks
        memory in long-lived workers that run many flows back-to-back."""
        with cls._pool_lock:
            if cls._connection_pool is not None:
                try:
                    cls._connection_pool.closeall()
                    logger.info("Closed QuestDB connection pool")
                except Exception as e:
                    logger.warning(f"Error closing connection pool: {e}")
                finally:
                    cls._connection_pool = None

    @classmethod
    def _reset_pool(cls):
        """Reset the connection pool when QuestDB becomes unreachable.
        Next connect() call will recreate the pool with fresh connections."""
        with cls._pool_lock:
            if cls._connection_pool is not None:
                try:
                    cls._connection_pool.closeall()
                except Exception:
                    pass
                cls._connection_pool = None
                logger.warning("Connection pool reset — will recreate on next connect")
    
    def ensure_connection(self, retries=3):
        """Ensure database connection is alive, reconnect if needed with retry logic"""
        try:
            # Check if connection exists and is alive
            if self.conn is None or self.conn.closed or self.cursor is None or self.cursor.closed:
                logger.warning("Database connection lost, reconnecting...")
                self.connect(retries=retries)
        except Exception as e:
            logger.error(f"Failed to ensure connection: {e}")
            self.connect(retries=retries)
    
    def close(self):
        """Close database connection (return to pool if using pool)"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if QuestDBClient._connection_pool is not None:
                # Return connection to pool
                QuestDBClient._connection_pool.putconn(self.conn)
                logger.debug("Returned connection to pool")
            else:
                # Close single connection
                self.conn.close()
                logger.info("Disconnected from QuestDB")
    
    def get_existing_timestamps(self, symbol: str, interval: str, table: str,
                                retries=2) -> set:
        """
        Get existing timestamps for a symbol/interval to avoid duplicates
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            interval: Data interval (e.g., 'd', '5m')
            table: Table from which existing timestamps are read
            retries: Number of retry attempts for mmap failures
        
        Returns:
            Set of existing timestamps
        """
        last_error = None
        for attempt in range(retries):
            try:
                self.ensure_connection()
                self.cursor.execute(f"""
                    SELECT DISTINCT timestamp 
                    FROM {table}
                    WHERE symbol = %s AND interval = %s
                """, (symbol, interval))
                return {row[0] for row in self.cursor.fetchall()}
            except Exception as e:
                last_error = e
                if attempt < retries - 1:
                    # Retry on mmap failures (memory pressure)
                    if "mmap" in str(e).lower():
                        time.sleep(0.5)  # Brief pause for memory to free up
                        logger.debug(f"Retrying timestamp query for {symbol}/{interval} (attempt {attempt + 2}/{retries})")
                        continue
                # Log warning only on final failure
                logger.warning(f"Could not query existing timestamps for {symbol}/{interval}: {last_error}")
                return set()  # Return empty set, duplicates handled by QuestDB
    
    def get_max_timestamp(self, symbol: str, interval: str) -> Optional[datetime]:
        """
        Get the latest data timestamp for a stock/interval from metadata
        
        Args:
            symbol: Stock symbol
            interval: Data interval
        
        Returns:
            Latest timestamp or None if no metadata exists
        """
        try:
            self.ensure_connection()
            # Get latest metadata record (in case of duplicates)
            self.cursor.execute(f"""
                SELECT data_end 
                FROM {TABLE_STOCK_METADATA} 
                WHERE symbol = %s AND interval = %s
                ORDER BY last_updated DESC
                LIMIT 1
            """, (symbol, interval))
            
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"Could not query max timestamp for {symbol}/{interval}: {e}")
            return None
    
    def delete_records_after_date(self, symbol: str, interval: str, from_date: datetime):
        """
        No-op kept for backward compatibility.

        QuestDB does not support row-level DELETE (the old ``DELETE FROM`` here
        always failed with "unexpected token [FROM]"). The eodhd_stock_data table
        has deduplication enabled with UPSERT KEYS (symbol, interval, timestamp),
        so re-inserting bars in the correction window overwrites the existing rows
        in place. An explicit delete is therefore unnecessary.

        Args:
            symbol: Stock symbol
            interval: Data interval
            from_date: Start of the correction window (informational only)
        """
        logger.debug(
            f"Skipping delete for {symbol}/{interval} from {from_date}: "
            f"QuestDB DEDUP (symbol, interval, timestamp) overwrites rows on re-insert"
        )
    
    def upsert_stock_metadata(self, symbol: str, interval: str, 
                             data_start: datetime, data_end: datetime, 
                             total_records: int):
        """
        Insert or update stock metadata for tracking data freshness
        
        Args:
            symbol: Stock symbol
            interval: Data interval
            data_start: Earliest timestamp in data
            data_end: Latest timestamp in data
            total_records: Total number of records
        """
        try:
            self.ensure_connection()
            now = datetime.now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

            # Check if we already have a row for today (designated timestamp = last_updated)
            self.cursor.execute(f"""
                SELECT last_updated FROM {TABLE_STOCK_METADATA}
                WHERE symbol = %s AND interval = %s AND last_updated >= %s
                ORDER BY last_updated DESC LIMIT 1
            """, (symbol, interval, today_start))
            existing = self.cursor.fetchone()

            if existing:
                # UPDATE the existing row in today's partition to avoid accumulation
                self.cursor.execute(f"""
                    UPDATE {TABLE_STOCK_METADATA}
                    SET total_records = %s, data_start = %s, data_end = %s
                    WHERE symbol = %s AND interval = %s AND last_updated = %s
                """, (total_records, data_start, data_end, symbol, interval, existing[0]))
            else:
                self.cursor.execute(f"""
                    INSERT INTO {TABLE_STOCK_METADATA}
                    (symbol, interval, last_updated, total_records, data_start, data_end, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (symbol, interval, now, total_records, data_start, data_end, now))

            logger.debug(f"Updated metadata for {symbol}/{interval}: {total_records} records, latest: {data_end}")
        except Exception as e:
            # Don't raise - metadata tracking is optional
            logger.warning(f"Could not upsert metadata for {symbol}/{interval}: {e}")
    
    def get_stocks_to_update(self, symbols: List[str], intervals: List[str], 
                            max_age_days: int = 1) -> Dict[str, List[str]]:
        """
        Get stocks that need updating based on data age
        
        Args:
            symbols: List of stock symbols
            intervals: List of intervals to check
            max_age_days: Maximum age in days before considering data stale
        
        Returns:
            Dict mapping symbols to list of intervals that need updating
        """
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        stocks_to_update = {}
        
        try:
            for symbol in symbols:
                intervals_to_update = []
                
                for interval in intervals:
                    # Check if metadata exists and is fresh
                    self.cursor.execute(f"""
                        SELECT data_end
                        FROM {TABLE_STOCK_METADATA}
                        WHERE symbol = %s AND interval = %s
                        ORDER BY last_updated DESC
                        LIMIT 1
                    """, (symbol, interval))
                    
                    result = self.cursor.fetchone()
                    
                    if not result or not result[0]:
                        # No metadata - needs full collection
                        intervals_to_update.append(interval)
                    elif result[0] < cutoff_date:
                        # Data is stale - needs update
                        intervals_to_update.append(interval)
                    # else: data is fresh - skip
                
                if intervals_to_update:
                    stocks_to_update[symbol] = intervals_to_update
            
            return stocks_to_update
            
        except Exception as e:
            logger.error(f"Failed to get stocks to update: {e}")
            # On error, return all stocks (safe fallback)
            return {symbol: intervals for symbol in symbols}
    
    def check_data_freshness(self, symbols: List[str], interval: str = 'd',
                              max_age_minutes: int = 60) -> Dict[str, list]:
        """
        Bulk freshness check for screener integration.
        
        Uses a single efficient query to check which tickers have fresh data
        and which need updating.
        
        Args:
            symbols: List of stock symbols to check
            interval: Data interval to check ('d', '5m', '1h', etc.)
            max_age_minutes: Maximum age in minutes before data is considered stale
                             For daily data: 1440 (24h) is reasonable
                             For intraday: 60-120 minutes
        
        Returns:
            Dict with keys:
                'fresh': list of symbols with up-to-date data
                'stale': list of symbols with outdated data  
                'unknown': list of symbols with no metadata (never collected)
        """
        result = {'fresh': [], 'stale': [], 'unknown': []}
        cutoff = datetime.now() - timedelta(minutes=max_age_minutes)
        
        try:
            self.ensure_connection()
            
            # Build a lookup of latest data_end per symbol from metadata
            # Use a single query for all symbols (much faster than N queries)
            if not symbols:
                return result
            
            placeholders = ','.join(['%s'] * len(symbols))
            self.cursor.execute(f"""
                SELECT symbol, data_end, last_updated
                FROM {TABLE_STOCK_METADATA}
                WHERE symbol IN ({placeholders})
                AND interval = %s
                ORDER BY symbol, last_updated DESC
            """, (*symbols, interval))
            
            rows = self.cursor.fetchall()
            
            # Build lookup: symbol -> latest data_end (first row per symbol due to ORDER BY DESC)
            seen = set()
            metadata_map = {}
            for row in rows:
                sym = row[0]
                if sym not in seen:
                    metadata_map[sym] = {
                        'data_end': row[1],
                        'last_updated': row[2]
                    }
                    seen.add(sym)
            
            # Classify each symbol
            for symbol in symbols:
                if symbol not in metadata_map:
                    result['unknown'].append(symbol)
                elif metadata_map[symbol]['data_end'] is None:
                    result['unknown'].append(symbol)
                elif metadata_map[symbol]['data_end'] < cutoff:
                    result['stale'].append(symbol)
                else:
                    result['fresh'].append(symbol)
            
            logger.info(
                f"Freshness check ({interval}, {max_age_minutes}min): "
                f"{len(result['fresh'])} fresh, {len(result['stale'])} stale, "
                f"{len(result['unknown'])} unknown"
            )
            return result
            
        except Exception as e:
            logger.error(f"Freshness check failed: {e}")
            # Safe fallback: treat all as stale
            return {'fresh': [], 'stale': list(symbols), 'unknown': []}
    
    def _screen_records(self, records, table):
        """
        Last checkpoint before a row reaches the table.

        Placed here rather than in the collectors because this is the one function
        every price writer already calls — collectors, flows and the loose backfill
        scripts alike. Fixing the rules at each call site was tried and did not
        hold: the next writer reimplemented the old behaviour, which is how four
        files ended up with seventeen copies of the same three rules.

        Only the empty-bar case is dropped, and that only because it was verified
        against the whole table: of 3,875,539 all-NULL rows, not one carries volume.
        Everything else is REPORTED AND KEPT. A gate that silently discards rows
        trades corruption for invisible data loss, and absence cannot be detected
        or repaired later the way a bad value can. If the counts below turn out to
        be large or surprising, the rule is wrong — not the data.
        """
        if not records or isinstance(records[0], dict):
            return records

        kept, reasons = [], Counter()
        for rec in records:
            reason = describe_rejection(rec)
            if reason is None:
                kept.append(rec)
            elif reason.startswith('bar kosong'):
                reasons[reason] += 1          # dropped: proven to carry nothing
            else:
                reasons[f'{reason} (TETAP DITULIS)'] += 1
                kept.append(rec)

        if reasons:
            summary = ', '.join(f'{r}: {n}' for r, n in reasons.most_common())
            logger.warning(f"[{table}] penyaringan {len(records)} baris -> {summary}")
        return kept

    def insert_price_data(self, records, table: str):
        """
        Insert price data records using ILP (fastest) or SQL fallback

        Args:
            records: List of tuples (symbol, interval, timestamp, open, high, low,
                    close, adjusted_close, volume, gmtoffset, source, created_at)
                    OR List of dicts (backward compatible)
            table: Required target table. Callers must choose production, legacy,
                   or a migration table explicitly.
        """
        if not records:
            return

        records = self._screen_records(records, table)
        if not records:
            return

        # Try ILP first (10-100x faster) if enabled
        if self.use_ilp and QuestDBClient._use_ilp:
            try:
                self._insert_price_data_ilp(records, table)
                return  # Success!
            except Exception as e:
                error_msg = str(e)
                # Only disable ILP permanently if it's a configuration issue, not a transient error
                if 'Connection refused' in error_msg or 'Name or service not known' in error_msg:
                    logger.warning(f"ILP configuration error, disabling permanently: {e}")
                    QuestDBClient._use_ilp = False
                    self.use_ilp = False
                else:
                    # Transient error - just log and fall back to SQL for this insert
                    logger.debug(f"ILP insert failed (transient), using SQL fallback: {e}")
        
        # Fallback to SQL insert (still fast with execute_batch)
        self._insert_price_data_sql(records, table)
    
    def _insert_price_data_ilp(self, records, table: str):
        """Insert using QuestDB ILP protocol (10-100x faster than SQL)"""
        if not HAS_ILP:
            raise ImportError("questdb library not available")
        
        # Convert records to appropriate format
        if records and isinstance(records[0], dict):
            # Dict format
            rows = records
        else:
            # Tuple format - convert to dicts for easier processing
            rows = []
            for r in records:
                rows.append({
                    'symbol': r[0],
                    'interval': r[1],
                    'timestamp': r[2],
                    'open': r[3],
                    'high': r[4],
                    'low': r[5],
                    'close': r[6],
                    'adjusted_close': r[7],
                    'volume': r[8],
                    'gmtoffset': r[9],
                    'source': r[10],
                    'created_at': r[11]
                })
        
        # Use thread lock to prevent concurrent ILP operations (prevents "Broken pipe")
        # Multiple threads sharing one ILP connection causes connection errors
        with QuestDBClient._ilp_lock:
            # Retry logic for transient errors
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    # Create new connection with auto_flush for better reliability
                    with Sender(Protocol.Tcp, self.ilp_host, self.ilp_port, auto_flush_rows=500) as sender:
                        for row in rows:
                            # Convert to TimestampNanos (QuestDB ILP requirement)
                            # Naive timestamps in this table are UTC by contract
                            # (intraday) or plain dates (EOD). datetime.timestamp()
                            # resolves a naive value using the PROCESS timezone, so
                            # the same row lands in a different place depending on
                            # where the collector runs: correct from the UTC
                            # container, 7 hours early from the WIB host. Pinning
                            # UTC makes the write independent of that.
                            ts = row['timestamp']
                            if isinstance(ts, datetime):
                                if ts.tzinfo is None:
                                    ts = ts.replace(tzinfo=timezone.utc)
                                ts_nanos = TimestampNanos(int(ts.timestamp() * 1_000_000_000))
                            else:
                                ts_nanos = TimestampNanos(int(ts * 1_000_000_000))
                            
                            # Build ILP row - all values must be correct types
                            # volume must be int (LONG in QuestDB), gmtoffset must be int (INT in QuestDB)
                            # Sending float for integer columns causes ILP cast error and row rejection
                            sender.row(
                                table,
                                symbols={
                                    'symbol': str(row['symbol']),
                                    'interval': str(row['interval']),
                                    'source': str(row.get('source', 'unknown'))
                                },
                                columns={
                                    'open': float(row['open']) if row.get('open') is not None else None,
                                    'high': float(row['high']) if row.get('high') is not None else None,
                                    'low': float(row['low']) if row.get('low') is not None else None,
                                    'close': float(row['close']) if row.get('close') is not None else None,
                                    'adjusted_close': float(row['adjusted_close']) if row.get('adjusted_close') is not None else None,
                                    'volume': int(row['volume']) if row.get('volume') is not None else None,
                                    'gmtoffset': int(row['gmtoffset']) if row.get('gmtoffset') is not None else None,
                                    # When the row was WRITTEN, as opposed to when the bar
                                    # happened. It was read out of the record above but
                                    # never sent, so every row that came through ILP — the
                                    # default, and 10-100x faster than the SQL fallback —
                                    # left it NULL: 13,167,158 rows, 62.1% of the table.
                                    #
                                    # That blindness is why tracing damage in this table
                                    # took guesswork. Where it WAS populated it worked:
                                    # bad monthly bars clustered in created_at 2026-02 and
                                    # 2026-06, which named aggregate_4h and backfill_gap as
                                    # the sources. For the other 92% there was no postmark.
                                    'created_at': _as_utc(row.get('created_at')),
                                },
                                at=ts_nanos
                            )
                        
                        # Explicit flush at the end (auto_flush handles batches)
                        sender.flush()
                    
                    logger.info(f"Inserted {len(records)} price records via ILP (fast)")
                    return  # Success - exit function
                    
                except Exception as e:
                    error_msg = str(e)
                    if attempt < max_retries - 1 and ('Broken pipe' in error_msg or 'Connection' in error_msg):
                        # Retry on connection errors
                        logger.debug(f"ILP retry (attempt {attempt + 1}/{max_retries}): {error_msg}")
                        time.sleep(0.1)  # Brief delay before retry
                        continue
                    else:
                        # Final attempt failed or non-connection error
                        raise Exception(f"ILP insert failed after {attempt + 1} attempts: {error_msg}")
    
    def _insert_price_data_sql(self, records, table: str):
        """Insert using SQL (slower but compatible fallback)"""
        sql = f"""
        INSERT INTO {table} 
        (symbol, interval, timestamp, open, high, low, close, adjusted_close, 
         volume, gmtoffset, source, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Check if records are tuples or dicts (backward compatibility)
        if records and isinstance(records[0], dict):
            # Convert dicts to tuples for backward compatibility
            values = []
            for record in records:
                values.append((
                    record['symbol'],
                    record['interval'],
                    record['timestamp'],
                    record['open'],
                    record['high'],
                    record['low'],
                    record['close'],
                    record.get('adjusted_close'),
                    record['volume'],
                    record.get('gmtoffset'),
                    record['source'],
                    _as_utc(record.get('created_at')).replace(tzinfo=None)
                ))
        else:
            # Already tuples - use directly, but created_at still has to be pinned
            # to UTC. Callers fill it with datetime.now(), which is the host clock;
            # leaving it alone would make this path store WIB while the ILP path
            # stores UTC, so the column's meaning would depend on which path ran.
            values = [tuple(r[:11]) + (_as_utc(r[11] if len(r) > 11 else None)
                                       .replace(tzinfo=None),)
                      for r in records]
        
        try:
            # Ensure connection is alive before batch insert
            self.ensure_connection()
            # Use execute_batch for better performance with large datasets
            execute_batch(self.cursor, sql, values, page_size=BATCH_INSERT_SIZE)
            logger.info(f"Inserted {len(records)} price records via SQL")
            
            # Small delay for large batches to prevent overwhelming QuestDB
            if len(records) > 5000:
                time.sleep(0.2)
                
        except Exception as e:
            logger.error(f"Failed to insert price data: {e}")
            # Try to reconnect and retry once with delay
            try:
                logger.warning("Reconnecting and retrying insert after delay...")
                time.sleep(1)  # Wait 1 second before reconnecting
                self.connect()
                time.sleep(0.5)  # Wait before retry
                execute_batch(self.cursor, sql, values, page_size=BATCH_INSERT_SIZE)
                logger.info(f"Inserted {len(records)} price records (after reconnect)")
                
                # Delay after successful retry
                if len(records) > 5000:
                    time.sleep(0.3)
                    
            except Exception as e2:
                logger.error(f"Failed to insert price data after reconnect: {e2}")
                raise
    
    def insert_corporate_actions(self, records: List[Dict], table: str):
        """
        Insert corporate actions (dividends/splits).

        Rejects rows whose action_date is missing or at epoch zero. Each collector
        already guards its own parsing, but this table accumulated 324,780 BRIS.JK
        rows dated 1970-01-01 from an earlier code path, so the guard belongs at the
        single point every writer passes through rather than in each caller.
        A dividend with no date is not a dividend — dropping it loses nothing.

        `table` is required so every caller declares its intended action store.
        """
        if not records:
            return

        sql = f"""
        INSERT INTO {table}
        (symbol, action_type, action_date, dividend_amount, dividend_currency,
         payment_date, record_date, declaration_date, dividend_type,
         split_ratio, split_from, split_to, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = []
        unparseable = 0
        implausible = 0
        for record in records:
            action_date, reason = _coerce_action_date(record.get('action_date'))
            if action_date is None:
                if reason == 'unparseable':
                    unparseable += 1
                else:
                    implausible += 1
                continue
            values.append((
                record['symbol'],
                record['action_type'],
                action_date,
                record.get('dividend_amount'),
                record.get('dividend_currency'),
                record.get('payment_date'),
                record.get('record_date'),
                record.get('declaration_date'),
                record.get('dividend_type'),
                record.get('split_ratio'),
                record.get('split_from'),
                record.get('split_to'),
                datetime.now()
            ))

        # Reported separately: an implausible date is bad data from upstream, an
        # unparseable one is a caller passing the wrong type. Collapsing them hides
        # a code bug behind a data-quality message.
        if implausible:
            logger.warning(f"Rejected {implausible} corporate action record(s) with "
                           f"missing or implausible action_date")
        if unparseable:
            logger.error(f"Rejected {unparseable} corporate action record(s) whose "
                         f"action_date could not be interpreted as a date — check the "
                         f"caller, it is passing the wrong type")
        if not values:
            return

        try:
            execute_batch(self.cursor, sql, values, page_size=BATCH_INSERT_SIZE)
            logger.info(f"Inserted {len(values)} corporate action records")
        except Exception as e:
            logger.error(f"Failed to insert corporate actions: {e}")
            raise
    
    def insert_or_update_metadata(self, symbol: str, data: Dict):
        """
        Insert or update stock metadata — one row per symbol.

        This table is current state (name, sector, is_active), not a time series, so
        it has no business date and QuestDB cannot deduplicate it: the dedup key must
        include the designated timestamp, and that is the insert time. An earlier
        version simply appended ("QuestDB doesn't support UPSERT easily"), which grew
        the table to 192,598 rows describing 953 symbols. Doing the upsert here is the
        only place it can be done, and mirrors upsert_stock_metadata().

        Descriptive fields are only overwritten when the caller actually supplies
        them. Most callers pass none, and blanking a name that another collector
        populated is how the accumulated rows ended up almost entirely empty.
        """
        if not data:
            return

        try:
            self.ensure_connection()
            now = datetime.now()

            self.cursor.execute(
                f"SELECT updated_at FROM {TABLE_METADATA} WHERE symbol = %s "
                f"ORDER BY updated_at DESC LIMIT 1",
                (symbol,)
            )
            existing = self.cursor.fetchone()

            if existing:
                sets = ['last_price_update = %s', 'is_active = %s']
                values = [data.get('last_price_update', now), True]
                if 'has_dividends' in data:
                    sets.append('has_dividends = %s')
                    values.append(data['has_dividends'])
                for column in ('exchange', 'name', 'sector', 'industry', 'currency'):
                    value = data.get(column)
                    if value:
                        sets.append(f'{column} = %s')
                        values.append(value)

                values.extend([symbol, existing[0]])
                self.cursor.execute(
                    f"UPDATE {TABLE_METADATA} SET {', '.join(sets)} "
                    f"WHERE symbol = %s AND updated_at = %s",
                    values
                )
            else:
                self.cursor.execute(
                    f"""INSERT INTO {TABLE_METADATA}
                        (symbol, exchange, name, sector, industry, currency,
                         last_price_update, has_dividends, is_active, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        symbol,
                        data.get('exchange', 'JK'),
                        data.get('name', ''),
                        data.get('sector', ''),
                        data.get('industry', ''),
                        data.get('currency', 'IDR'),
                        data.get('last_price_update', now),
                        data.get('has_dividends', False),
                        True,
                        now,
                        now,
                    )
                )
            logger.debug(f"Upserted metadata for {symbol}")
        except Exception as e:
            logger.warning(f"Could not upsert metadata for {symbol}: {e}")
