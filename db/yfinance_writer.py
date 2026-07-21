"""
QuestDB writer for yfinance-sourced tables.

Kept separate from questdb_client.py: that client is the hot path for 26M price
rows and is shared by every collection flow. This is a different data domain
(quarterly/daily snapshots, ~400K rows) with its own tables, so it does not
belong in the same file.

All four tables use DEDUP UPSERT KEYS, so re-running a sweep overwrites rather
than duplicating — the failure mode that produced 330K duplicate metadata rows.
"""

import logging
from datetime import datetime
from typing import Dict, List

import psycopg2
from psycopg2.extras import execute_batch

from config.db_config import (
    BATCH_INSERT_SIZE,
    QUESTDB_HOST,
    QUESTDB_PG_PORT,
    QUESTDB_USER,
    QUESTDB_PASSWORD,
    QUESTDB_DATABASE,
)
from config.yfinance_config import (
    TABLE_YF_FUNDAMENTALS,
    TABLE_YF_VALUATION,
    TABLE_YF_ANALYST,
    TABLE_YF_PROFILE,
    VALUATION_FIELDS,
    PROFILE_FIELDS,
)

logger = logging.getLogger(__name__)

VALUATION_COLUMNS = list(VALUATION_FIELDS.values())
PROFILE_COLUMNS = list(PROFILE_FIELDS.values()) + ['isin']

ANALYST_COLUMNS = [
    'period', 'eps_avg', 'eps_low', 'eps_high',
    'revenue_avg', 'revenue_low', 'revenue_high',
    'num_analysts', 'growth',
    'target_current', 'target_high', 'target_low', 'target_mean', 'target_median',
    'currency',
]


class YFinanceWriter:
    """Batch writer for the yf_* tables."""

    def __init__(self):
        self.conn = psycopg2.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PG_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database=QUESTDB_DATABASE,
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def close(self):
        for handle in (self.cursor, self.conn):
            try:
                handle.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- writers ------------------------------------------------------------

    def insert_fundamentals(self, records: List[Dict]) -> int:
        if not records:
            return 0

        sql = f"""
        INSERT INTO {TABLE_YF_FUNDAMENTALS}
        (symbol, period_end, statement, freq, line_item, value, value_kind,
         currency, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        now = datetime.now()
        values = [(
            r['symbol'], r['period_end'], r['statement'], r['freq'],
            r['line_item'], r['value'], r['value_kind'], r.get('currency'), now,
        ) for r in records]

        return self._execute(sql, values, TABLE_YF_FUNDAMENTALS)

    def insert_valuation(self, records: List[Dict], ts: datetime) -> int:
        if not records:
            return 0

        cols = (['symbol', 'ts'] + VALUATION_COLUMNS
                + ['currency', 'financial_currency', 'ingested_at'])
        sql = f"""
        INSERT INTO {TABLE_YF_VALUATION} ({', '.join(cols)})
        VALUES ({', '.join(['%s'] * len(cols))})
        """
        now = datetime.now()
        values = [
            tuple([r['symbol'], ts] + [r.get(c) for c in VALUATION_COLUMNS]
                  + [r.get('currency'), r.get('financial_currency'), now])
            for r in records
        ]

        return self._execute(sql, values, TABLE_YF_VALUATION)

    def insert_analyst(self, records: List[Dict], ts: datetime) -> int:
        if not records:
            return 0

        cols = ['symbol', 'ts'] + ANALYST_COLUMNS + ['ingested_at']
        sql = f"""
        INSERT INTO {TABLE_YF_ANALYST} ({', '.join(cols)})
        VALUES ({', '.join(['%s'] * len(cols))})
        """
        now = datetime.now()
        values = [
            tuple([r['symbol'], ts] + [r.get(c) for c in ANALYST_COLUMNS] + [now])
            for r in records
        ]

        return self._execute(sql, values, TABLE_YF_ANALYST)

    def insert_profile(self, records: List[Dict], ts: datetime) -> int:
        if not records:
            return 0

        cols = ['symbol', 'ts'] + PROFILE_COLUMNS + ['ingested_at']
        sql = f"""
        INSERT INTO {TABLE_YF_PROFILE} ({', '.join(cols)})
        VALUES ({', '.join(['%s'] * len(cols))})
        """
        now = datetime.now()
        values = [
            tuple([r['symbol'], ts] + [r.get(c) for c in PROFILE_COLUMNS] + [now])
            for r in records
        ]

        return self._execute(sql, values, TABLE_YF_PROFILE)

    # -- internals ----------------------------------------------------------

    def _execute(self, sql: str, values: List[tuple], table: str) -> int:
        try:
            execute_batch(self.cursor, sql, values, page_size=BATCH_INSERT_SIZE)
            logger.info(f"Inserted {len(values)} rows into {table}")
            return len(values)
        except Exception as e:
            logger.error(f"Failed to insert into {table}: {e}")
            raise
