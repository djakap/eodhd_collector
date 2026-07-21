"""
Prefect Flow: daily read-only census of eodhd_stock_data.

This exists because of a question worth recording: "every time I ask for a check
there is damage — why does it keep happening?" The answer was that nothing checked
routinely. Damage accumulated for months and each ad-hoc check surfaced a different
stratum of the same backlog, which felt like fresh corruption every time.

Run daily, the report becomes a boring delta. That is the point: after the backlog
is cleared, anything appearing here is a real signal rather than an excavation.

The flow writes nothing. It wraps scripts/data_audit.py, captures the anomaly
counts, and fails the run only when a class GROWS — a new unknown interval, or
more empty bars than yesterday — so a quiet day stays quiet in the UI.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import json
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import Dict

from prefect import flow, task, get_run_logger

from scripts.data_audit import Audit

STATE_PATH = Path("reports/data_audit_state.json")


@task(name="Run data audit", retries=1, retry_delay_seconds=120)
def run_audit(table: str) -> Dict:
    log = get_run_logger()
    audit = Audit(table)

    # The census prints a full report; keep it in the run log rather than the
    # console so a failure can be read back later without re-running it.
    buf = io.StringIO()
    with redirect_stdout(buf):
        audit.run()
    report = buf.getvalue()
    for line in report.splitlines():
        log.info(line)

    counts = {}
    for cls, n, detail in audit.findings:
        counts[f"{cls} | {detail}"] = n
    return {'counts': counts, 'total': sum(counts.values())}


@task(name="Compare against yesterday")
def compare(result: Dict) -> Dict:
    log = get_run_logger()
    counts = result['counts']

    previous = {}
    if STATE_PATH.exists():
        try:
            previous = json.loads(STATE_PATH.read_text()).get('counts', {})
        except (ValueError, OSError):
            log.warning("state sebelumnya tidak terbaca — dianggap run pertama")

    grown, appeared = [], []
    for key, n in counts.items():
        before = previous.get(key)
        if before is None:
            appeared.append((key, n))
        elif n > before:
            grown.append((key, before, n))

    if not previous:
        # Everything is "new" when there is nothing to compare against, so a first
        # run records the baseline instead of reporting every class as a regression.
        log.info(f"Run pertama — {len(counts)} kelas anomali, "
                 f"{result['total']:,} baris. Ini garis dasarnya, bukan kemunduran.")
        grown, appeared = [], []
    elif not grown and not appeared:
        log.info(f"Tidak ada perubahan sejak run terakhir "
                 f"({result['total']:,} baris tersentuh anomali).")
    else:
        for key, n in appeared:
            log.error(f"KELAS BARU: {key} = {n:,}")
        for key, before, n in grown:
            log.error(f"BERTAMBAH: {key}  {before:,} -> {n:,}  (+{n - before:,})")

    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps({
        'generated': datetime.now().isoformat(timespec='seconds'),
        'counts': counts,
    }, indent=2))

    return {'grown': grown, 'appeared': appeared, 'total': result['total']}


@flow(name="Data Audit", log_prints=True)
def data_audit_flow(table: str = "eodhd_stock_data") -> Dict:
    log = get_run_logger()
    result = run_audit(table)
    delta = compare(result)

    regressions = len(delta['grown']) + len(delta['appeared'])
    if regressions:
        # Raising marks the run failed so it is visible in the UI. The data is
        # untouched either way — this flow only reads.
        raise RuntimeError(
            f"{regressions} kelas anomali baru atau bertambah — lihat log. "
            f"Tidak ada data yang diubah.")

    log.info(f"Audit bersih: {delta['total']:,} baris tersentuh anomali, "
             f"tidak ada yang bertambah.")
    return delta


if __name__ == "__main__":
    data_audit_flow()
