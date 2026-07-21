-- EODHD Data Collector - QuestDB Schema
-- 5 Tables for comprehensive stock data collection
-- Uses same QuestDB instance with 'eodhd_' prefix to avoid conflicts
--
-- ############################################################################
-- KNOWN DESIGN DEFECT in tables 2-6 below: they use an INSERT-time column
-- (created_at / updated_at / last_updated) as the designated timestamp instead
-- of the business date the row describes.
--
-- QuestDB requires the designated timestamp to be part of DEDUP UPSERT KEYS.
-- When that timestamp is the insert time it differs on every re-fetch, so DEDUP
-- can never match and these tables cannot be made idempotent without being
-- rebuilt around their business date. Verified 2026-07-20.
--
-- The cost is measurable: eodhd_corporate_actions holds 584,764 rows for 4,584
-- real facts (128x), eodhd_metadata 192,598 rows for 953 symbols (202x).
--
-- Table 1 below does it correctly — the designated timestamp is the bar time,
-- which is why DEDUP works there. Follow that pattern for any new table.
-- ############################################################################

-- ============================================================================
-- 1. PRICE DATA TABLE
-- ============================================================================
CREATE TABLE eodhd_stock_data (
    symbol SYMBOL,              -- Stock symbol (e.g., 'BBCA.JK')
    interval SYMBOL,            -- '5m', '15m', '30m', '1h', 'd', 'w', 'm'
    timestamp TIMESTAMP,        -- Bar timestamp (designated timestamp), stored UTC
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adjusted_close DOUBLE,      -- NULL for intraday, populated for EOD
    volume LONG,
    gmtoffset INT,              -- GMT offset (from intraday API), NULL for EOD
    source SYMBOL,              -- 'eod' or 'intraday'
    created_at TIMESTAMP        -- When record was inserted
) timestamp(timestamp) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(timestamp, symbol, interval);

-- PARTITION BY MONTH, not DAY: at DAY granularity this table reaches ~13,000
-- partitions instead of 443, and partition count is a direct driver of the file
-- handle and mmap pressure that has crashed this instance.
--
-- DEDUP makes re-collection idempotent — required for the EODHD/yfinance parallel
-- run, where both sources write the same (symbol, interval, timestamp).
--
-- QuestDB automatically indexes SYMBOL columns, no need for explicit indexes

-- ============================================================================
-- 2. FUNDAMENTALS TABLE
-- ============================================================================
CREATE TABLE eodhd_fundamentals (
    symbol SYMBOL,
    report_date DATE,
    report_type SYMBOL,         -- 'quarterly' or 'annual'
    
    -- Valuation Metrics
    market_cap DOUBLE,
    enterprise_value DOUBLE,
    pe_ratio DOUBLE,
    pb_ratio DOUBLE,
    ps_ratio DOUBLE,
    peg_ratio DOUBLE,
    ev_ebitda DOUBLE,
    
    -- Income Statement
    revenue DOUBLE,
    gross_profit DOUBLE,
    operating_income DOUBLE,
    net_income DOUBLE,
    ebitda DOUBLE,
    eps DOUBLE,
    
    -- Margins
    gross_margin DOUBLE,
    operating_margin DOUBLE,
    profit_margin DOUBLE,
    
    -- Returns
    roe DOUBLE,
    roa DOUBLE,
    roic DOUBLE,
    
    -- Balance Sheet
    total_assets DOUBLE,
    total_liabilities DOUBLE,
    total_equity DOUBLE,
    cash DOUBLE,
    debt DOUBLE,
    
    -- Cash Flow
    operating_cash_flow DOUBLE,
    free_cash_flow DOUBLE,
    capex DOUBLE,
    
    -- Dividends
    dividend_per_share DOUBLE,
    dividend_yield DOUBLE,
    payout_ratio DOUBLE,
    
    -- Ratios
    current_ratio DOUBLE,
    quick_ratio DOUBLE,
    debt_to_equity DOUBLE,
    
    -- Growth
    revenue_growth DOUBLE,
    earnings_growth DOUBLE,
    
    -- Metadata
    currency SYMBOL,
    updated_at TIMESTAMP,
    created_at TIMESTAMP
) timestamp(created_at) PARTITION BY YEAR WAL;

-- QuestDB automatically indexes SYMBOL columns

-- ============================================================================
-- 3. CORPORATE ACTIONS TABLE
-- ============================================================================
-- Rebuilt 2026-07-20 around action_date. The original used timestamp(created_at) —
-- the insert time — which made DEDUP impossible (QuestDB requires the designated
-- timestamp in the dedup key, and the insert time differs on every re-fetch). The
-- table had grown to 584,764 rows holding 4,584 distinct facts, 324,780 of them
-- epoch-zero junk. See scripts/rebuild_corporate_actions.py.
--
-- NOTE on dividend_amount: the collectors store EODHD's `value`, which is the
-- ADJUSTED dividend and is recomputed upstream over time — so the same fact can
-- legitimately return a different amount on a later fetch. DEDUP now overwrites
-- with the newest value instead of appending a second row.
CREATE TABLE eodhd_corporate_actions (
    symbol SYMBOL,
    action_type SYMBOL,         -- 'dividend' or 'split'
    action_date TIMESTAMP,      -- business date (designated timestamp)

    -- Dividend Fields
    dividend_amount DOUBLE,
    dividend_currency SYMBOL,
    payment_date TIMESTAMP,
    record_date TIMESTAMP,
    declaration_date TIMESTAMP,
    dividend_type SYMBOL,

    -- Split Fields
    split_ratio STRING,
    split_from INT,
    split_to INT,

    -- Metadata
    created_at TIMESTAMP
) timestamp(action_date) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(action_date, symbol, action_type);

-- QuestDB automatically indexes SYMBOL columns

-- ============================================================================
-- 4. CALENDAR EVENTS TABLE
-- ============================================================================
CREATE TABLE eodhd_calendar_events (
    symbol SYMBOL,
    event_type SYMBOL,          -- 'earnings', 'dividend', 'ipo', 'split'
    event_date DATE,
    
    -- Earnings Fields
    estimated_eps DOUBLE,
    actual_eps DOUBLE,
    surprise_pct DOUBLE,
    before_after_market SYMBOL,
    
    -- Dividend Fields
    expected_dividend DOUBLE,
    ex_dividend_date DATE,
    
    -- IPO Fields
    ipo_price DOUBLE,
    ipo_shares LONG,
    
    -- Split Fields
    expected_split_ratio STRING,
    
    -- Status
    status SYMBOL,              -- 'scheduled', 'confirmed', 'completed'
    
    -- Metadata
    updated_at TIMESTAMP,
    created_at TIMESTAMP
) timestamp(created_at) PARTITION BY YEAR WAL;

-- QuestDB automatically indexes SYMBOL columns

-- ============================================================================
-- 5. METADATA TABLE
-- ============================================================================
CREATE TABLE eodhd_metadata (
    symbol SYMBOL,
    exchange SYMBOL,
    name STRING,
    sector SYMBOL,
    industry SYMBOL,
    currency SYMBOL,
    
    -- Collection Status
    last_price_update TIMESTAMP,
    last_fundamental_update TIMESTAMP,
    last_action_update TIMESTAMP,
    last_calendar_update TIMESTAMP,
    
    -- Data Availability
    has_fundamentals BOOLEAN,
    has_dividends BOOLEAN,
    has_splits BOOLEAN,
    
    -- Statistics
    total_price_records LONG,
    total_fundamental_records INT,
    total_dividends INT,
    total_splits INT,
    
    -- Price Range
    earliest_price_date TIMESTAMP,
    latest_price_date TIMESTAMP,
    
    -- Errors
    last_error STRING,
    error_count INT,
    
    -- Metadata
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) timestamp(updated_at) PARTITION BY YEAR WAL;

-- QuestDB automatically indexes SYMBOL columns

-- ============================================================================
-- 6. STOCK METADATA TABLE (for update mode tracking)
-- ============================================================================
CREATE TABLE eodhd_stock_metadata (
    symbol SYMBOL,
    interval SYMBOL,            -- 'd', 'w', 'm', '5m', '15m', '30m', '1h'
    last_updated TIMESTAMP,     -- When this stock/interval was last collected
    total_records LONG,         -- Total records for this stock/interval
    data_start TIMESTAMP,       -- Earliest timestamp in database
    data_end TIMESTAMP,         -- Latest timestamp in database (for update filtering)
    created_at TIMESTAMP        -- When first created
) timestamp(last_updated) PARTITION BY DAY WAL;

-- QuestDB automatically indexes SYMBOL columns
