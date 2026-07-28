"""
Prefect Flow: nightly data check + heal for the yfinance production table.

This is what makes the migration watch self-monitoring instead of something the
user checks by hand every night. It runs after the evening price collection and,
in one pass:

  1. QUALITY  — the read-only census (scripts/data_audit), compared day-over-day.
                Fails if an anomaly class grows or appears (new corruption).
  2. COMPLETE — is the most recent settled trading day present for ~all symbols?
                A settled day is the newest date at least 90% of symbols reached,
                which avoids false alarms on today's still-publishing bar.
  3. HEAL     — fetch just the symbols missing that day (bounded). The nightly
                21:30 update already self-heals via its last-bar lookback; this
                is the safety net for a run that failed outright (e.g., the laptop
                slept), so a gap does not wait for the next weekday.
  4. VERDICT  — fail loudly (visible in the Prefect UI) if quality regressed, if
                data is STALE (the settled day is too far behind), or if a real
                gap persists after healing.

Symbols that simply are not served by yfinance (delisted, or illiquid with no
recent bar) will keep lagging and are reported, not treated as failures — that is
a known source limitation, not a pipeline fault.
"""

import io
import os
import sys
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from prefect import flow, task, get_run_logger

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)
from flows.fundamentals_flow import load_symbols
from flows.data_audit_flow import run_audit, compare      # reuse the quality census
from api.yfinance_client import RateLimitedError
from collectors.yfinance_price_collector import YFinancePriceCollector

PROD = 'stock_data'
COVERAGE_FLOOR = 0.90        # a "settled" day is one ≥90% of symbols reached
STALE_DAYS = 4               # settled day older than this many calendar days = STALE
HEAL_CAP = 150               # more laggards than this = systemic; alert, don't hammer


def _connect():
    return psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                            password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)


@task(name="Check completeness")
def check_completeness(stocks_file: str) -> Dict:
    log = get_run_logger()
    symbols = set(load_symbols(stocks_file, None))
    conn = _connect()
    cur = conn.cursor()

    # last daily bar per symbol
    cur.execute(f"SELECT symbol, max(timestamp) FROM {PROD} WHERE interval='d' GROUP BY symbol")
    last = {s: t for s, t in cur.fetchall() if s in symbols}

    # settled reference day: newest date reached by ≥ COVERAGE_FLOOR of symbols
    from collections import Counter
    day_counts = Counter(str(t)[:10] for t in last.values())
    settled = None
    for day in sorted(day_counts, reverse=True):
        reached = sum(1 for t in last.values() if str(t)[:10] >= day)
        if reached >= len(symbols) * COVERAGE_FLOOR:
            settled = day
            break
    conn.close()

    if settled is None:
        return {'settled': None, 'laggards': [], 'coverage': 0, 'stale_days': 999}

    laggards = sorted(s for s, t in last.items() if str(t)[:10] < settled)
    coverage = len(symbols) - len(laggards)
    stale_days = (datetime.now().date() - datetime.strptime(settled, '%Y-%m-%d').date()).days

    log.info(f"Hari settled: {settled} | cakupan {coverage}/{len(symbols)} "
             f"| tertinggal {len(laggards)} | umur {stale_days} hari")
    if laggards:
        log.info(f"  tertinggal (contoh): {laggards[:15]}")
    return {'settled': settled, 'laggards': laggards,
            'coverage': coverage, 'total': len(symbols), 'stale_days': stale_days}


@task(name="Heal missing symbols", retries=1, retry_delay_seconds=180)
def heal(laggards: List[str]) -> Dict:
    log = get_run_logger()
    if not laggards:
        return {'attempted': 0, 'healed': 0}
    if len(laggards) > HEAL_CAP:
        log.warning(f"{len(laggards)} simbol tertinggal — di atas batas {HEAL_CAP}. "
                    f"Ini sistemik; tidak menyembuhkan otomatis, cukup melaporkan.")
        return {'attempted': 0, 'healed': 0, 'systemic': True}

    log.info(f"Menyembuhkan {len(laggards)} simbol via fetch bertarget")
    healed = 0
    with YFinancePriceCollector(update_mode=True) as col:
        for sym in laggards:
            try:
                r = col.collect_all(sym, skip_intraday=False, skip_actions=True)
                if r['eod'] or r['intraday']:
                    healed += 1
            except RateLimitedError as e:
                log.error(f"Rate limit di {sym}: {e} — berhenti menyembuhkan")
                break
            except Exception as e:
                log.error(f"{sym}: {type(e).__name__}: {e}")
    log.info(f"Sembuh: {healed}/{len(laggards)}")
    return {'attempted': len(laggards), 'healed': healed}


@task(name="Quality census")
def quality(table: str) -> Dict:
    log = get_run_logger()
    result = run_audit.fn(table)          # reuse the read-only census
    delta = compare.fn(result)            # day-over-day comparison + history record
    return delta


@flow(name="Nightly Data Check", log_prints=True)
def nightly_check_flow(stocks_file: str = "config/syariah_stocks.txt") -> Dict:
    log = get_run_logger()

    # 1. quality (read-only census, fails on regression inside compare via history)
    q = quality(PROD)
    quality_regressed = bool(q.get('grown') or q.get('appeared'))

    # 2 + 3. completeness, then heal the gap, then re-check
    before = check_completeness(stocks_file)
    healed = heal(before['laggards'])
    after = check_completeness(stocks_file) if healed.get('healed') else before

    # 4. verdict
    problems = []
    if quality_regressed:
        problems.append(f"kualitas mundur: {q['grown']} {q['appeared']}")
    if after['stale_days'] >= STALE_DAYS:
        problems.append(f"data STALE — hari settled {after['settled']} "
                        f"berumur {after['stale_days']} hari (koleksi malam mungkin gagal)")
    # persistent laggards are only a problem if there are many (systemic), since a
    # handful are always delisted/illiquid symbols yfinance cannot serve
    if len(after['laggards']) > HEAL_CAP:
        problems.append(f"{len(after['laggards'])} simbol tertinggal setelah heal — sistemik")

    log.info(f"RINGKASAN: settled={after['settled']} cakupan={after['coverage']}/{after['total']} "
             f"tertinggal={len(after['laggards'])} disembuhkan={healed.get('healed',0)}")

    if problems:
        raise RuntimeError("CEK MALAM GAGAL — " + " ; ".join(problems)
                           + ". Tidak ada data yang rusak; ini peringatan agar diperiksa.")

    log.info("Cek malam bersih: kualitas stabil, data lengkap & terkini.")
    return {'settled': after['settled'], 'coverage': after['coverage'],
            'laggards': len(after['laggards']), 'healed': healed.get('healed', 0)}


if __name__ == "__main__":
    nightly_check_flow()
