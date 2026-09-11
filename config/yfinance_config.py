# yfinance Collector Configuration
#
# Tables use a 'yf_' prefix (same rationale as the 'eodhd_' prefix in db_config):
# the QuestDB instance is shared, so tables are namespaced by source.

import os
from dotenv import load_dotenv

from config.tables import TABLE_PRICES_PRODUCTION

load_dotenv()

# Source-specific yfinance tables. Source-neutral production and legacy table
# authority is defined in config.tables.
TABLE_YF_FUNDAMENTALS = 'yf_fundamentals'
TABLE_YF_VALUATION = 'yf_valuation_daily'
TABLE_YF_ANALYST = 'yf_analyst_snapshot'
TABLE_YF_PROFILE = 'yf_profile'

# ---------------------------------------------------------------------------
# Throttling
#
# yfinance is an unofficial scraper; Yahoo rate-limits by IP and request
# pattern (YFRateLimitError). A fundamentals sweep hits several endpoints per
# symbol, so it is far heavier than price collection and needs its own budget.
# Measured safe pacing during evaluation: ~0.3-0.5s between endpoint calls.
# ---------------------------------------------------------------------------
YF_REQUEST_DELAY = float(os.getenv('YF_REQUEST_DELAY', 0.4))   # seconds between calls
YF_JITTER = float(os.getenv('YF_JITTER', 0.25))                # random 0..J added, breaks lockstep patterns
YF_SYMBOL_DELAY = float(os.getenv('YF_SYMBOL_DELAY', 0.5))     # extra pause between symbols

YF_MAX_RETRIES = int(os.getenv('YF_MAX_RETRIES', 4))
YF_BACKOFF_BASE = float(os.getenv('YF_BACKOFF_BASE', 5.0))     # 5s, 10s, 20s, 40s
YF_BACKOFF_MAX = float(os.getenv('YF_BACKOFF_MAX', 120.0))

# Abort the whole sweep if this many consecutive symbols fail — means we are
# being blocked outright and hammering further only deepens the ban.
YF_CIRCUIT_BREAKER = int(os.getenv('YF_CIRCUIT_BREAKER', 15))

# ---------------------------------------------------------------------------
# Unit conventions
#
# yfinance is INTERNALLY INCONSISTENT about ratio units. Verified empirically
# against IDX symbols (BRIS/ANTM/TLKM/PGAS, July 2026):
#
#   dividendYield        1.72      -> PERCENT   (TLKM cross-check: reported 8.39
#                                                vs computed 8.23% -> ratio 1.019)
#   payoutRatio          0.1333    -> fraction
#   returnOnEquity       0.16151   -> fraction
#   profitMargins        0.31929   -> fraction
#   heldPercentInsiders  0.92121   -> fraction, despite "Percent" in the name
#
# We normalise everything to FRACTIONS on ingest so downstream code has one
# convention. Columns are named to match what is actually stored.
# ---------------------------------------------------------------------------
YF_PERCENT_FIELDS = {'dividendYield'}   # divided by 100 at ingest

# info -> yf_valuation_daily column mapping.
VALUATION_FIELDS = {
    'marketCap': 'market_cap',
    'enterpriseValue': 'enterprise_value',
    'sharesOutstanding': 'shares_outstanding',
    'floatShares': 'float_shares',
    'trailingPE': 'trailing_pe',
    'forwardPE': 'forward_pe',
    'priceToBook': 'price_to_book',
    'bookValue': 'book_value',
    'trailingEps': 'trailing_eps',
    'forwardEps': 'forward_eps',
    'dividendYield': 'dividend_yield',          # normalised to fraction
    'payoutRatio': 'payout_ratio',
    'beta': 'beta',
    'returnOnEquity': 'return_on_equity',
    'returnOnAssets': 'return_on_assets',
    'profitMargins': 'profit_margins',
    'totalRevenue': 'total_revenue',
    'netIncomeToCommon': 'net_income_to_common',
    'averageVolume': 'average_volume',
    'fiftyTwoWeekHigh': 'fifty_two_week_high',
    'fiftyTwoWeekLow': 'fifty_two_week_low',
    'heldPercentInsiders': 'held_frac_insiders',   # fraction, not percent
    'heldPercentInstitutions': 'held_frac_institutions',
}

PROFILE_FIELDS = {
    'longName': 'long_name',
    'sector': 'sector',
    'industry': 'industry',
    'currency': 'currency',
    'exchange': 'exchange',
    'quoteType': 'quote_type',
    'fullTimeEmployees': 'full_time_employees',
    'website': 'website',
}

# ---------------------------------------------------------------------------
# Price collection
#
# Interval codes are the EODHD ones already stored in eodhd_stock_data.interval;
# api/yfinance_client.INTERVAL_MAP translates them for Yahoo.
#
# Intraday defaults to 1h only. Yahoo caps 5m/15m/30m at 60 days (measured), so
# they can be kept current but never re-fetched further back — the existing 5m
# archive from 2024-06 is not reproducible from this source. 4h is not collected:
# utils/aggregate_4h derives it from 1h against the database.
# ---------------------------------------------------------------------------
YF_EOD_PERIODS = ['d', 'w', 'm']
YF_INTRADAY_INTERVALS = ['1h']

# Where the price collector writes. Production is the default; the environment
# override remains available for an explicitly selected scratch/comparison table.
YF_PRICE_TABLE = os.getenv('YF_PRICE_TABLE', TABLE_PRICES_PRODUCTION)

# How far back a full (non-incremental) collection reaches.
YF_EOD_PERIOD_FULL = 'max'
YF_INTRADAY_FULL_DAYS = 700       # under Yahoo's 730-day cap for 1h

# Incremental runs re-fetch this many days before the last stored bar, so late
# corrections and adjustments are picked up. DEDUP makes the overlap free.
YF_UPDATE_WINDOW_DAYS = 7

# Statements pulled per symbol: (yfinance attribute, statement, freq)
STATEMENTS = [
    ('income_stmt', 'income', 'annual'),
    ('balance_sheet', 'balance', 'annual'),
    ('cash_flow', 'cashflow', 'annual'),
    ('quarterly_income_stmt', 'income', 'quarterly'),
    ('quarterly_balance_sheet', 'balance', 'quarterly'),
    ('quarterly_cash_flow', 'cashflow', 'quarterly'),
]
