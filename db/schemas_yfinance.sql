-- yfinance Collector - QuestDB Schema
-- Tables prefixed 'yf_' (shared QuestDB instance, namespaced by source)
--
-- Why these exist: yfinance only retains 4-6 fundamental periods and a SINGLE
-- snapshot of valuation/analyst metrics. Those point-in-time values are
-- overwritten and unrecoverable once they change. Snapshotting them here builds
-- a history that cannot be bought back later.

-- ============================================================================
-- 0. PRICE SHADOW TABLE (parallel-run only)
-- ============================================================================
-- Mirrors eodhd_stock_data exactly so the two sources can be compared bar for bar.
--
-- Why a separate table rather than writing both sources into eodhd_stock_data:
-- that table dedups on (timestamp, symbol, interval), so the second writer simply
-- overwrites the first. There would be nothing left to compare — the very thing
-- the parallel run exists to measure.
--
-- After cutover this table is dropped and the collector is pointed at
-- eodhd_stock_data by clearing YF_PRICE_TABLE.
CREATE TABLE yf_stock_data (
    symbol SYMBOL,
    interval SYMBOL,            -- 'd', 'w', 'm', '1h', ...
    timestamp TIMESTAMP,        -- bar time, UTC for intraday (designated)
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adjusted_close DOUBLE,
    volume LONG,
    gmtoffset INT,              -- always NULL from yfinance
    source SYMBOL,              -- 'eod' or 'intraday'
    created_at TIMESTAMP
) timestamp(timestamp) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(timestamp, symbol, interval);

-- ============================================================================
-- 1. FUNDAMENTALS (long/narrow)
-- ============================================================================
-- Narrow, not wide, because line items differ per issuer: a bank reports no
-- 'Current Ratio', a miner reports items a bank never will. A wide table would
-- be mostly NULL and would need a migration every time Yahoo adds a line item.
--
-- value_kind exists because a single statement MIXES units: 'Net Income' is
-- absolute IDR, 'Tax Rate For Calcs' is a ratio, 'Basic EPS' is per-share, and
-- 'Ordinary Shares Number' is a share count. Never aggregate across kinds.
CREATE TABLE yf_fundamentals (
    symbol SYMBOL,              -- 'BRIS.JK'
    period_end TIMESTAMP,       -- fiscal period end (designated timestamp)
    statement SYMBOL,           -- 'income' | 'balance' | 'cashflow'
    freq SYMBOL,                -- 'annual' | 'quarterly'
    line_item SYMBOL,           -- e.g. 'Net Income', 'Total Debt'
    value DOUBLE,
    value_kind SYMBOL,          -- 'absolute' | 'ratio' | 'per_share' | 'count'
    currency SYMBOL,            -- REPORTING currency, not the trading currency.
                                -- IDX coal issuers (AADI, ADRO, ADMR...) report in
                                -- USD while trading in IDR. Never compare absolute
                                -- values across issuers without checking this.
    ingested_at TIMESTAMP
) timestamp(period_end) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(period_end, symbol, statement, freq, line_item);

-- ============================================================================
-- 2. VALUATION SNAPSHOT (daily)
-- ============================================================================
-- One row per symbol per day from Ticker.info. This is the table that ACCUMULATES
-- what yfinance itself throws away.
--
-- UNITS: all ratio-like columns are FRACTIONS (0.16 = 16%). yfinance reports
-- dividendYield as a percent while everything else is a fraction; it is divided
-- by 100 at ingest. heldPercentInsiders is already a fraction despite its name,
-- hence 'held_frac_*' here.
CREATE TABLE yf_valuation_daily (
    symbol SYMBOL,
    ts TIMESTAMP,               -- snapshot date (designated timestamp)
    market_cap DOUBLE,
    enterprise_value DOUBLE,
    shares_outstanding DOUBLE,
    float_shares DOUBLE,
    trailing_pe DOUBLE,
    forward_pe DOUBLE,
    price_to_book DOUBLE,
    book_value DOUBLE,
    trailing_eps DOUBLE,
    forward_eps DOUBLE,
    dividend_yield DOUBLE,      -- FRACTION (normalised from yfinance percent)
    payout_ratio DOUBLE,        -- fraction
    beta DOUBLE,
    return_on_equity DOUBLE,    -- fraction
    return_on_assets DOUBLE,    -- fraction
    profit_margins DOUBLE,      -- fraction
    total_revenue DOUBLE,
    net_income_to_common DOUBLE,
    average_volume DOUBLE,
    fifty_two_week_high DOUBLE,
    fifty_two_week_low DOUBLE,
    held_frac_insiders DOUBLE,      -- fraction
    held_frac_institutions DOUBLE,  -- fraction
    currency SYMBOL,                -- trading currency (IDR for .JK)
    financial_currency SYMBOL,      -- REPORTING currency
    -- When financial_currency != currency, Yahoo's derived ratios are WRONG: it
    -- divides an IDR price by a USD book value without converting. Verified
    -- 2026-07: AADI.JK price=8850 IDR / bookValue=0.446 USD -> priceToBook=19843.
    -- Filter on `financial_currency = currency` before trusting trailing_pe,
    -- forward_pe, price_to_book, book_value, trailing_eps or forward_eps.
    ingested_at TIMESTAMP
) timestamp(ts) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(ts, symbol);

-- ============================================================================
-- 3. ANALYST SNAPSHOT
-- ============================================================================
-- Price targets + EPS/revenue estimates per forecast period, stamped with the
-- snapshot date. Tracking how estimates get revised is the point; yfinance only
-- ever shows the current value.
CREATE TABLE yf_analyst_snapshot (
    symbol SYMBOL,
    ts TIMESTAMP,               -- snapshot date (designated timestamp)
    period SYMBOL,              -- '0q' | '+1q' | '0y' | '+1y' | 'target'
    eps_avg DOUBLE,
    eps_low DOUBLE,
    eps_high DOUBLE,
    revenue_avg DOUBLE,
    revenue_low DOUBLE,
    revenue_high DOUBLE,
    num_analysts DOUBLE,
    growth DOUBLE,              -- fraction
    target_current DOUBLE,
    target_high DOUBLE,
    target_low DOUBLE,
    target_mean DOUBLE,
    target_median DOUBLE,
    currency SYMBOL,
    ingested_at TIMESTAMP
) timestamp(ts) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(ts, symbol, period);

-- ============================================================================
-- 4. PROFILE (slowly changing)
-- ============================================================================
CREATE TABLE yf_profile (
    symbol SYMBOL,
    ts TIMESTAMP,               -- refresh date (designated timestamp)
    long_name STRING,
    sector SYMBOL,
    industry SYMBOL,
    currency SYMBOL,
    exchange SYMBOL,
    quote_type SYMBOL,
    isin STRING,
    full_time_employees LONG,
    website STRING,
    ingested_at TIMESTAMP
) timestamp(ts) PARTITION BY YEAR WAL
  DEDUP UPSERT KEYS(ts, symbol);
