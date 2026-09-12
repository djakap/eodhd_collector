"""Authoritative names for production and legacy market-data tables.

"Production" means the table written by active Prefect schedules and read by
downstream consumers, as measured on 2026-09-11. It does not imply that the
table is verified complete: corporate-action completeness remains an open
reconciliation question for QCF-013.
"""

TABLE_PRICES_PRODUCTION = 'stock_data'
TABLE_PRICES_LEGACY_EODHD = 'eodhd_stock_data'
TABLE_PRICE_METADATA_PRODUCTION = 'stock_metadata'
TABLE_PRICE_METADATA_LEGACY_EODHD = 'eodhd_stock_metadata'
TABLE_ACTIONS_PRODUCTION = 'corporate_actions'
TABLE_ACTIONS_LEGACY_EODHD = 'eodhd_corporate_actions'
