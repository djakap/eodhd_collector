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
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Mapping

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
from scripts.check_adjustment_consistency import inspect_adjustment_consistency
from scripts.check_table_partitions import inspect_all_tables
from scripts.heal_suspended_wal import heal as heal_wal
from scripts.repair_adjustments import (
    ADJUSTMENT_HEAL_CAP,
    apply_plan,
    plan_repairs,
)

PROD = 'stock_data'
COVERAGE_FLOOR = 0.90        # a "settled" day is one ≥90% of symbols reached
STALE_DAYS = 4               # settled day older than this many calendar days = STALE
HEAL_CAP = 150               # more laggards than this = systemic; alert, don't hammer


def _connect():
    return psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
                            password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE)


@task(name="Check completeness")
def check_completeness(stocks_file: str) -> Dict:
    """Measure daily completeness against the authoritative universe.

    Required data means a daily (``interval='d'``) bar on or after the
    settled day.  The settled day is the newest stored date reached by at
    least ``COVERAGE_FLOOR`` of the full universe, including zero-row symbols
    in the denominator.  This keeps weekends and an unfinished current
    session from advancing the reference day.

    ``missing_entirely`` contains universe members with no daily rows and
    ``laggards`` contains members with rows older than the reference day.
    Both lower ``coverage_ratio``, but remain separate so operators can tell
    absence from staleness and healing can target both conditions.
    """
    log = get_run_logger()
    symbols = set(load_symbols(stocks_file, None))
    conn = _connect()
    cur = conn.cursor()

    # last daily bar per symbol
    cur.execute(f"SELECT symbol, max(timestamp) FROM {PROD} WHERE interval='d' GROUP BY symbol")
    last = {s: t for s, t in cur.fetchall() if s in symbols}

    conn.close()

    result = _compute_completeness(symbols, last)

    log.info(f"Hari settled: {result['settled']} "
             f"| cakupan {result['coverage']}/{result['total']} "
             f"({result['coverage_ratio']:.2%}) "
             f"| tertinggal {len(result['laggards'])} "
             f"| tanpa data {len(result['missing_entirely'])} "
             f"| umur {result['stale_days']} hari")
    if result['laggards']:
        log.info(f"  tertinggal (contoh): {result['laggards'][:15]}")
    if result['missing_entirely']:
        log.info(f"  tanpa data (contoh): {result['missing_entirely'][:15]}")
    return result


def _compute_completeness(
    universe: Iterable[str],
    newest_day_per_symbol: Mapping[str, object],
    *,
    as_of_date: date | None = None,
) -> Dict:
    """Return deterministic daily-completeness state for supplied observations.

    ``as_of_date`` affects only ``stale_days``.  Supplying it makes fixtures
    independent of wall-clock time; settled-day selection depends solely on
    the universe and stored daily observations.
    """
    symbols = set(universe)
    total = len(symbols)
    newest_days = {
        symbol: str(timestamp)[:10]
        for symbol, timestamp in newest_day_per_symbol.items()
        if symbol in symbols
    }
    missing_entirely = sorted(symbols - newest_days.keys())

    settled = None
    for day in sorted(set(newest_days.values()), reverse=True):
        reached = sum(1 for newest in newest_days.values() if newest >= day)
        if reached >= total * COVERAGE_FLOOR:
            settled = day
            break

    reference_day = settled or max(newest_days.values(), default=None)
    laggards = sorted(
        symbol
        for symbol, newest in newest_days.items()
        if reference_day is not None and newest < reference_day
    )
    coverage = total - len(missing_entirely) - len(laggards)
    coverage_ratio = coverage / total if total else 0.0

    if reference_day is None:
        stale_days = 999
    else:
        current_date = as_of_date or datetime.now().date()
        stale_days = (
            current_date - datetime.strptime(reference_day, '%Y-%m-%d').date()
        ).days

    return {
        'settled': settled,
        'total': total,
        'coverage': coverage,
        'coverage_ratio': coverage_ratio,
        'laggards': laggards,
        'missing_entirely': missing_entirely,
        'stale_days': stale_days,
    }


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


@task(name="Self-heal suspended WAL")
def self_heal_wal() -> int:
    """Resume any QuestDB table whose WAL apply has been suspended.

    A corrupt WAL segment — the signature of an unclean shutdown on this laptop —
    suspends the table and silently freezes its data: on 2026-08-03 that had
    hidden a MONTH of stock_data behind a stack that looked perfectly healthy.
    The worker only heals this at startup, so a suspension beginning mid-day
    would otherwise sit undetected until the next restart; checking here caps
    that exposure at one day.

    Note this deliberately does NOT also run clear_orphan_runs: that marks every
    non-terminal run terminal, which from inside a flow would kill this very run.
    Orphan clearing stays a worker-startup step.
    """
    log = get_run_logger()
    healed = heal_wal()
    if healed:
        log.warning(f"WAL tersuspend dipulihkan: {healed} tabel")
    return healed


@task(name="Check partition readability")
def check_partition_readability() -> int:
    """Report metadata/readability divergence; never attempt table repair."""
    log = get_run_logger()
    problems = 0
    for finding in inspect_all_tables():
        if finding.problem:
            problems += 1
            log.error(finding.message)
        elif finding.known_defect:
            log.warning(finding.message)
        elif finding.deferred:
            log.warning(finding.message)
        else:
            log.info(finding.message)
    return problems


@task(name="Check adjustment consistency")
def check_adjustment_consistency() -> int:
    """Report corporate-action adjustment defects; never rewrite prices (QCF-004)."""
    log = get_run_logger()
    try:
        findings = inspect_adjustment_consistency()
    except Exception as exc:
        log.error(f"Pemeriksaan penyesuaian harga gagal dijalankan: {type(exc).__name__}: {exc}")
        return 1
    problems = 0
    for finding in findings:
        if finding.problem:
            problems += 1
            log.error(finding.message)
        elif finding.known_defect:
            log.warning(finding.message)
        else:
            log.info(finding.message)
    return problems


@task(name="Heal adjustment defects")
def heal_adjustments() -> Dict:
    """Re-fetch / correct history behind corporate actions (DATA-003); registered runs are left alone."""
    log = get_run_logger()
    summary = {"planned": 0, "applied": 0, "deferred": 0}
    try:
        with YFinancePriceCollector(update_mode=False) as collector:
            plan = plan_repairs(
                connection=None,
                provider=collector.api,
                cap=ADJUSTMENT_HEAL_CAP,
            )
            plan["mode"] = "nightly"
            summary["planned"] = plan["target_count"]
            summary["deferred"] = plan["deferred"]
            record = apply_plan(plan, collector)
            summary["applied"] = len(record["targets"])
            if record.get("record_file"):
                summary["record_file"] = record["record_file"]
            for target in record["targets"]:
                log.info(
                    f"Adjustment heal {target['symbol']}: "
                    f"{', '.join(target['reasons'])}"
                )
    except RateLimitedError as exc:
        record = getattr(exc, "record", {})
        summary["applied"] = len(record.get("targets", []))
        if record.get("record_file"):
            summary["record_file"] = record["record_file"]
        summary["error"] = f"RateLimitedError: {exc}"
        log.error(f"Adjustment heal berhenti karena rate limit: {exc}")
    except Exception as exc:
        record = getattr(exc, "record", {})
        summary["applied"] = len(record.get("targets", []))
        if record.get("record_file"):
            summary["record_file"] = record["record_file"]
        summary["error"] = f"{type(exc).__name__}: {exc}"
        log.error(f"Adjustment heal gagal: {type(exc).__name__}: {exc}")
    log.info(
        f"Adjustment heal: planned={summary['planned']} "
        f"applied={summary['applied']} deferred={summary['deferred']}"
    )
    return summary


@flow(name="Nightly Data Check", log_prints=True)
def nightly_check_flow(stocks_file: str = "config/syariah_stocks.txt") -> Dict:
    log = get_run_logger()

    # 0. un-stick any suspended WAL before anything reads stock_data — a
    #    suspended table serves stale data that looks perfectly plausible.
    self_heal_wal()

    # Suspended WAL and partition-directory divergence are distinct faults. The
    # latter is report-only: repair remains a deliberate, evidence-backed action.
    partition_problems = check_partition_readability()

    heal_adjustments()

    # The consistency guard reports anything DATA-003's heal left behind.
    adjustment_problems = check_adjustment_consistency()

    # 1. quality (read-only census, fails on regression inside compare via history)
    q = quality(PROD)
    quality_regressed = bool(q.get('grown') or q.get('appeared'))

    # 2 + 3. completeness, then heal the gap, then re-check
    before = check_completeness(stocks_file)
    before_candidates = sorted(set(before['laggards']) | set(before['missing_entirely']))
    healed = heal(before_candidates)
    after = check_completeness(stocks_file) if healed.get('healed') else before

    # 4. verdict
    problems = []
    if partition_problems > 0:
        problems.append(f"{partition_problems} tabel bermasalah pada pemeriksaan partisi")
    if adjustment_problems > 0:
        problems.append(f"{adjustment_problems} temuan penyesuaian harga belum terdaftar")
    if quality_regressed:
        problems.append(f"kualitas mundur: {q['grown']} {q['appeared']}")
    if after['settled'] is None:
        problems.append(f"hari settled tidak ditemukan — "
                        f"{len(after['missing_entirely'])} simbol tanpa data")
    elif after['stale_days'] >= STALE_DAYS:
        problems.append(f"data STALE — hari settled {after['settled']} "
                        f"berumur {after['stale_days']} hari (koleksi malam mungkin gagal)")
    # persistent laggards are only a problem if there are many (systemic), since a
    # handful are always delisted/illiquid symbols yfinance cannot serve
    after_candidates = set(after['laggards']) | set(after['missing_entirely'])
    if len(after_candidates) > HEAL_CAP:
        problems.append(f"{len(after_candidates)} simbol tertinggal/tanpa data "
                        f"setelah heal — sistemik")

    log.info(f"RINGKASAN: settled={after['settled']} cakupan={after['coverage']}/{after['total']} "
             f"({after['coverage_ratio']:.2%}) tertinggal={len(after['laggards'])} "
             f"tanpa_data={len(after['missing_entirely'])} "
             f"disembuhkan={healed.get('healed',0)}")

    if problems:
        raise RuntimeError("CEK MALAM GAGAL — " + " ; ".join(problems)
                           + ". Tidak ada data yang rusak; ini peringatan agar diperiksa.")

    log.info("Cek malam bersih: kualitas stabil, data lengkap & terkini.")
    return {'settled': after['settled'], 'coverage': after['coverage'],
            'laggards': len(after['laggards']), 'healed': healed.get('healed', 0)}


if __name__ == "__main__":
    nightly_check_flow()
