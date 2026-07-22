#!/usr/bin/env python3
"""
Decide what the 124,999 out-of-session rows actually are.

They sit outside IDX hours (02:00-09:00 UTC for intraday, midnight for EOD) and
have been carried through every cleanup untouched, because "outside market hours"
is a suspicion, not evidence. Twice already in this effort a class that looked
like junk turned out to be real: the 15m and 30m bars at 00:00-01:00 UTC are
byte-identical to what EODHD serves for those days, and a plan to "correct" them
would have moved 376,763 rows away from the source.

So this asks the only authority that can settle it — the API — and asks it per
class rather than per row. Every row in a class shares an interval and an hour;
if the API returns a bar at that exact timestamp for a sample of them, the class
is real data with an unusual clock, and deleting it would destroy good bars.

Read-only. Produces a verdict per class; removal, if any, is a separate step.

Usage:  python scripts/adjudicate_out_of_session.py [--sample 12]
"""

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import psycopg2
from dotenv import load_dotenv

from config.db_config import (
    TABLE_STOCK_DATA, QUESTDB_HOST, QUESTDB_PG_PORT,
    QUESTDB_USER, QUESTDB_PASSWORD, QUESTDB_DATABASE,
)

load_dotenv()
BASE = "https://eodhd.com/api"
SESSION = range(2, 10)          # 02:00-09:00 UTC
INTRADAY = ('5m', '15m', '1h')


def api_stamps(key, symbol, interval, day):
    """Timestamps EODHD returns for this symbol/interval/day, as 'YYYY-MM-DD HH:MM'."""
    start = int(datetime.strptime(day, '%Y-%m-%d')
                .replace(tzinfo=timezone.utc).timestamp())
    r = requests.get(f"{BASE}/intraday/{symbol}", params={
        'api_token': key, 'interval': interval, 'fmt': 'json',
        'from': start - 86400, 'to': start + 2 * 86400}, timeout=45)
    if r.status_code != 200:
        return None
    try:
        return {d['datetime'][:16] for d in r.json()}
    except (ValueError, KeyError, TypeError):
        return None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--sample', type=int, default=12,
                   help="bar diuji per kelas (interval, jam)")
    args = p.parse_args()

    key = os.getenv('EODHD_API_KEY', '')
    if not key:
        sys.exit("EODHD_API_KEY tidak ada di .env")

    conn = psycopg2.connect(host=QUESTDB_HOST, port=QUESTDB_PG_PORT,
                            user=QUESTDB_USER, password=QUESTDB_PASSWORD,
                            database=QUESTDB_DATABASE)
    cur = conn.cursor()

    classes = defaultdict(list)
    for iv in INTRADAY + ('d',):
        inside = "hour(timestamp) = 0" if iv == 'd' else \
                 f"hour(timestamp) >= {SESSION.start} AND hour(timestamp) < {SESSION.stop}"
        cur.execute(f"""SELECT symbol, timestamp, close, volume FROM "{TABLE_STOCK_DATA}"
                        WHERE interval = %s AND NOT ({inside})""", (iv,))
        for s, ts, cl, vol in cur.fetchall():
            classes[(iv, ts.hour)].append((s, ts, cl, vol))

    print(f"Kelas ditemukan: {len(classes)}\n")
    print(f"{'interval':9s} {'jam':>4s} {'baris':>9s} {'ada isi':>9s}  verdikt")

    for (iv, hour), rows in sorted(classes.items()):
        with_data = sum(1 for _, _, cl, _ in rows if cl is not None)

        if iv == 'd':
            # Daily bars belong at midnight; the API has no notion of a 17:00
            # daily bar to check against, so judge on content alone.
            verdict = ("kosong — aman dibuang" if with_data == 0
                       else "PERLU PERIKSA — ada isi")
            print(f"{iv:9s} {hour:>4d} {len(rows):>9,} {with_data:>9,}  {verdict}")
            continue

        seen, hits, misses, failed = set(), 0, 0, 0
        for s, ts, cl, vol in rows:
            if len(seen) >= args.sample:
                break
            day = str(ts)[:10]
            if (s, day) in seen:
                continue
            seen.add((s, day))
            stamps = api_stamps(key, s, iv, day)
            time.sleep(0.15)
            if stamps is None:
                failed += 1
                continue
            if str(ts)[:16] in stamps:
                hits += 1
            else:
                misses += 1

        checked = hits + misses
        if not checked:
            verdict = "tidak bisa diuji"
        elif hits == checked:
            verdict = "ASLI — API mengembalikan bar ini, JANGAN dibuang"
        elif hits == 0:
            verdict = "sampah — API tidak mengenalnya"
        else:
            verdict = f"campur ({hits} asli / {misses} tidak dikenal)"
        print(f"{iv:9s} {hour:>4d} {len(rows):>9,} {with_data:>9,}  {verdict}")

    print("\n  Tidak ada data yang diubah oleh skrip ini.")


if __name__ == "__main__":
    main()
