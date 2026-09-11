# 2026-02 Flat-Layout Archive

This directory preserves evidence copied from the unversioned legacy tree at
`/home/djp/eodhd_collector`. The source files date from the February 2026 flat-layout
collector and were archived on 2026-09-11 before that legacy tree was renamed.

The 36 preserved files had no same-path counterpart in the canonical working tree, no path
in the canonical repository's Git history, and no matching content in the canonical working
tree or Git history. Their original relative paths and bytes are retained.

These files are historical evidence, **not working tools**. Do not execute, lint, repair, or
adopt them as current collector code. Several scripts assume the old flat layout and refer to
the retired `eodhd_stock_data` table.

The classifier also found 11 files identical to the canonical file at the same relative path.
Sixteen additional non-identical but non-unique files were deliberately left behind: 13 had
different content at the same current relative path, while three had a historical-path or
cross-path counterpart. They already exist in the canonical repository or represent
superseded versions, so they are not part of this archive.

In particular, `.env`, `.env.example`, `config/syariah_stocks.txt`, and the 4 MB
`logs/eodhd_collector.log` were not archived. The archive contains neither EODHD API key
value found in the legacy and canonical `.env` files.
