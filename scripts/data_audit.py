#!/usr/bin/env python3
"""
Step 0 of the data-quality plan: a read-only census of eodhd_stock_data.

This script WRITES NOTHING. It exists because every previous estimate in this
effort was made by sampling one month or one interval and generalising, and every
one of them was wrong by a large factor:

    all-NULL rows      estimated    53,242  ->  actual  3,875,539
    misdated 'm' bars  estimated    12,984  ->  actual     84,672
    "out-of-session"   assumed junk         ->  actual  real bars, shifted clock

So it measures the whole table, groups every anomaly into a named class, and for
each class reports enough evidence (counts, date span, sample rows, whether the
rows carry volume) to decide the remedy on facts rather than inference.

Two QuestDB 7.3.10 traps are avoided deliberately:
  * GROUP BY hour(timestamp) silently drops the WHERE range predicate and returns
    whole-table counts. hour() is used only as a predicate here, never grouped on.
  * Range predicates on the designated timestamp miss rows outside their
    partition; year() is pruning-immune and is used for the epoch checks.

Usage:  python scripts/data_audit.py [--table eodhd_stock_data]
"""

import argparse
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config.db_config import (
    QUESTDB_HOST, QUESTDB_PG_PORT, QUESTDB_USER,
    QUESTDB_PASSWORD, QUESTDB_DATABASE,
)

from utils.bar_rules import SENTINEL_MIN

OHLC_NULL = "open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL"

# Intervals we expect to exist. Anything outside this is itself a finding —
# 4h_null (17,321 rows, every field NULL) went unnoticed for months because
# nobody ever enumerated the column's actual values.
EXPECTED_INTERVALS = {'5m', '15m', '30m', '1h', '4h', 'd', 'w', 'm'}

# Intervals whose bars are stamped at midnight rather than within the session.
EOD_INTERVALS = {'d', 'w', 'm'}

# Hours each interval legitimately occupies, in UTC. IDX trades 02:00-09:00 UTC,
# but 15m is the exception: EODHD serves 15m bars at 01:00 and 10:00 too, and
# those 106,864 rows were confirmed real by querying the API for a sample of each
# class — it returns a bar at exactly those timestamps. They are listed here so
# the census stops reporting verified data as an anomaly. Everything else that
# fell outside the session was checked the same way, found unrecognised by the
# API, and removed (18,135 rows, see scripts/remove_out_of_session_junk.py).
SESSION_HOURS = {
    '5m':  set(range(2, 10)),
    '15m': set(range(2, 10)) | {1, 10},
    '1h':  set(range(2, 10)),
    '4h':  set(range(2, 10)),
}


def hr(title):
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


class Audit:
    def __init__(self, table):
        self.table = table
        self.conn = psycopg2.connect(
            host=QUESTDB_HOST, port=QUESTDB_PG_PORT, user=QUESTDB_USER,
            password=QUESTDB_PASSWORD, database=QUESTDB_DATABASE,
        )
        self.cur = self.conn.cursor()
        self.findings = []

    def one(self, sql, params=None):
        self.cur.execute(sql, params)
        row = self.cur.fetchone()
        return row[0] if row else None

    def all(self, sql, params=None):
        self.cur.execute(sql, params)
        return self.cur.fetchall()

    def note(self, cls, count, detail):
        self.findings.append((cls, count, detail))

    # ------------------------------------------------------------------
    def intervals(self):
        hr("1. INVENTARIS INTERVAL")
        rows = self.all(f'SELECT interval, count() FROM "{self.table}" GROUP BY interval')
        rows.sort(key=lambda r: -r[1])
        total = sum(r[1] for r in rows)
        print(f"{'interval':10s} {'baris':>12s} {'%':>6s} {'simbol':>7s}  rentang")
        for iv, n in rows:
            syms = self.one(f'SELECT count_distinct(symbol) FROM "{self.table}" WHERE interval = %s', (iv,))
            lo = self.one(f'SELECT min(timestamp) FROM "{self.table}" WHERE interval = %s', (iv,))
            hi = self.one(f'SELECT max(timestamp) FROM "{self.table}" WHERE interval = %s', (iv,))
            flag = '' if iv in EXPECTED_INTERVALS else '   <-- TIDAK DIKENAL'
            print(f"{iv:10s} {n:>12,} {n/total*100:>5.1f}% {syms:>7,}  "
                  f"{str(lo)[:10]} .. {str(hi)[:10]}{flag}")
            if iv not in EXPECTED_INTERVALS:
                self.note('interval tidak dikenal', n, f"interval='{iv}'")
        print(f"{'TOTAL':10s} {total:>12,}")
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    def null_rows(self, ivs):
        hr("2. BARIS TANPA OHLC")
        print("  Dua hal berbeda, dan membedakannya penting:")
        print("    KOSONG        tanpa OHLC dan tanpa volume — tidak berisi apa pun.")
        print("    HARGA HILANG  tanpa OHLC tapi VOLUMENYA NYATA. Ini hasil perbaikan")
        print("                  sentinel 999999.9999, bukan cacat: harganya memang")
        print("                  tidak disediakan EODHD, sementara volumenya asli.\n")
        print(f"{'interval':10s} {'kosong':>10s} {'harga hilang':>14s}  catatan")
        grand_empty = grand_priceless = 0
        for iv in ivs:
            n = self.one(f'SELECT count() FROM "{self.table}" WHERE interval = %s AND {OHLC_NULL}', (iv,))
            if not n:
                continue
            vol = self.one(f'SELECT count() FROM "{self.table}" WHERE interval = %s '
                           f'AND {OHLC_NULL} AND volume IS NOT NULL AND volume > 0', (iv,))
            empty = n - vol
            grand_empty += empty
            grand_priceless += vol
            note = "" if empty == 0 else "  <-- harus 0 setelah pembersihan"
            print(f"{iv:10s} {empty:>10,} {vol:>14,}{note}")
            # Only the truly empty ones are an anomaly. Counting the repaired rows
            # here would make the daily audit report its own fix as damage, forever.
            if empty:
                self.note('baris kosong', empty, f"interval={iv}")
        print(f"{'TOTAL':10s} {grand_empty:>10,} {grand_priceless:>14,}")
        if grand_priceless:
            print(f"\n  {grand_priceless:,} baris 'harga hilang' DIPERTAHANKAN dengan sengaja.")

        # the counterpart that must NOT be swept up with them
        keep = self.one(f'SELECT count() FROM "{self.table}" '
                        f'WHERE close IS NOT NULL AND volume IS NULL')
        print(f"  Pembanding — OHLC ADA tapi volume NULL: {keep:,} baris, juga sah.")


    # ------------------------------------------------------------------
    def sentinel_prices(self, ivs):
        hr("2b. HARGA SENTINEL 999999.9999 (EODHD: harga tidak tersedia)")
        print("  Tersimpan SEBAGAI HARGA, jadi perhitungan return atau dollar bar")
        print("  akan melihat kuotasi sejuta rupiah. Placeholder EODHD sendiri —")
        print("  API masih mengembalikannya untuk sebagian simbol (SMGR/TSPC/INCO)")
        print("  tapi kini memberi harga asli untuk yang lain (TLKM 1995-12-01).\n")
        grand = 0
        for iv in ivs:
            n = self.one(f'SELECT count() FROM "{self.table}" WHERE interval = %s '
                         f'AND (open >= {SENTINEL_MIN} OR high >= {SENTINEL_MIN} '
                         f'OR low >= {SENTINEL_MIN} OR close >= {SENTINEL_MIN})', (iv,))
            if n:
                lo = self.one(f'SELECT min(timestamp) FROM "{self.table}" '
                              f'WHERE interval = %s AND close >= {SENTINEL_MIN}', (iv,))
                hi = self.one(f'SELECT max(timestamp) FROM "{self.table}" '
                              f'WHERE interval = %s AND close >= {SENTINEL_MIN}', (iv,))
                print(f"  {iv:8s} {n:>8,}  {str(lo)[:10]} .. {str(hi)[:10]}")
                self.note('harga sentinel', n, f'interval={iv}')
                grand += n
        print(f"  {'TOTAL':8s} {grand:>8,}")

    def inflated_history(self):
        hr("2c. HARGA LAMA MENGGELEMBUNG")
        print("  Sebagian sejarah EODHD menyimpan harga yang jauh di atas kisaran")
        print("  saham itu sendiri — ANTM tercatat 996,548 di 2003 padahal kini ~1,000.")
        print("  Polanya: tiap simbol punya tanggal peralihan tajam, dan tidak ada satu")
        print("  pun baris seperti ini setelah 2010. Diukur terhadap harga simbol itu")
        print("  sendiri, bukan ambang tetap — DSSA 290,000 dan DCII 359,900 itu sah.\n")

        # Only symbols that hold a large old price can qualify, and there are few,
        # so the expensive per-symbol comparison runs on a short list.
        cands = self.all(
            f'SELECT symbol FROM (SELECT symbol, count() n FROM "{self.table}" '
            f"WHERE interval='d' AND close > 50000 AND close < {SENTINEL_MIN} "
            f"AND timestamp < '2010-01-01' GROUP BY symbol) WHERE n > 0")

        total, hits = 0, []
        for (sym,) in cands:
            rows = self.all(
                f'SELECT close FROM "{self.table}" WHERE symbol = %s AND interval=\'d\' '
                f"AND timestamp >= '2012-01-01' AND timestamp < '2016-01-01' "
                f"AND close IS NOT NULL", (sym,))
            if len(rows) < 100:
                continue
            vals = sorted(float(r[0]) for r in rows)
            ref = vals[len(vals) // 2]
            limit = ref * 20
            n = self.one(f'SELECT count() FROM "{self.table}" WHERE symbol = %s '
                         f"AND interval='d' AND close > {limit} "
                         f"AND close < {SENTINEL_MIN}", (sym,))
            if n:
                last = self.one(f'SELECT max(timestamp) FROM "{self.table}" '
                                f"WHERE symbol = %s AND interval='d' AND close > {limit} "
                                f"AND close < {SENTINEL_MIN}", (sym,))
                hits.append((sym, n, ref, str(last)[:10]))
                total += n

        if not hits:
            print("  tidak ada")
            return
        print(f"  {'simbol':10s} {'baris':>7s} {'acuan modern':>14s}  terakhir")
        for sym, n, ref, last in sorted(hits, key=lambda x: -x[1]):
            print(f"  {sym:10s} {n:>7,} {ref:>14,.0f}  {last}")
        print(f"  {'TOTAL':10s} {total:>7,}")
        self.note('harga lama menggelembung', total, f'{len(hits)} simbol, semua sebelum 2010')

    def hour_profile(self, ivs):
        hr("3. PROFIL JAM (UTC) — mendeteksi stempel waktu tergeser")
        print("  Sesi IDX 09:00-16:00 WIB = 02:00-09:00 UTC.\n")
        for iv in ivs:
            counts = []
            for h in range(24):
                n = self.one(f'SELECT count() FROM "{self.table}" '
                             f'WHERE interval = %s AND hour(timestamp) = {h}', (iv,))
                if n:
                    counts.append((h, n))
            if not counts:
                continue
            inside = SESSION_HOURS.get(iv, {0} if iv in EOD_INTERVALS else set(range(2, 10)))
            out = [(h, n) for h, n in counts if h not in inside]
            body = ' '.join(f"{h:02d}h={n:,}" for h, n in counts)
            print(f"  {iv:8s} {body}")
            if out:
                n_out = sum(n for _, n in out)
                print(f"  {'':8s}   -> di luar sesi: {n_out:,} baris pada jam "
                      f"{[h for h, _ in out]}")
                self.note('jam di luar sesi', n_out, f"interval={iv}, jam={[h for h,_ in out]}")

    # ------------------------------------------------------------------
    def shift_probe(self):
        hr("4. UJI GESERAN — apakah interval sepakat soal jam pembukaan?")
        print("  Bar pertama hari yang sama harus membuka pada harga yang sama.")
        print("  Jam yang berbeda dengan open yang sama = stempel tergeser, BUKAN sampah.\n")
        days = self.all(f'SELECT timestamp FROM "{self.table}" WHERE interval = %s '
                        f"AND timestamp >= '2026-03-01' AND timestamp < '2026-03-20' "
                        f"AND symbol = 'AADI.JK' AND close IS NOT NULL", ('1h',))
        sample_days = sorted({str(d[0])[:10] for d in days})[:3]

        for day in sample_days:
            print(f"  --- AADI.JK {day} ---")
            base_open = None
            for iv in ['5m', '15m', '30m', '1h']:
                rows = self.all(
                    f'SELECT timestamp, open FROM "{self.table}" WHERE symbol = %s '
                    f'AND interval = %s AND timestamp >= %s AND timestamp < %s '
                    f'AND close IS NOT NULL ORDER BY timestamp',
                    ('AADI.JK', iv, day, day + ' 23:59:59'))
                if not rows:
                    continue
                ts, op = rows[0]
                if iv == '1h':
                    base_open = op
                print(f"    {iv:5s} bar pertama {str(ts)[11:16]} UTC  open={op}")
            if base_open is not None:
                print(f"    -> semua open sama? bandingkan dengan 1h open={base_open}")

    # ------------------------------------------------------------------
    def period_alignment(self):
        hr("5. BAR w/m — berapa banyak per (simbol, periode)?")
        print("  CATATAN: 'bukan tanggal 1' BUKAN anomali. Diverifikasi terhadap API")
        print("  EODHD 2026-07-21: bar bulanan distempel pada hari bursa pertama, dan")
        print("  nilai tersimpan cocok persis dengan API (BRIS/UNTR 2025-01..04,")
        print("  termasuk tanggal ganjil 2025-04-08). Versi sebelumnya memeriksa")
        print("  date_trunc() dan menandai 84,672 baris sah sebagai rusak.\n")
        print("  Uji yang benar adalah kelebihan jumlah: EODHD mengembalikan SATU bar")
        print("  per periode, jadi periode dengan lebih dari satu baris memuat sisa")
        print("  pengambilan inkremental. Otoritasnya tetap API — lihat")
        print("  scripts/reconcile_periods.py untuk pencocokan baris-per-baris.\n")
        for iv, label in [('m', 'bulan'), ('w', 'minggu')]:
            tot = self.one(f'SELECT count() FROM "{self.table}" WHERE interval = %s', (iv,))
            uniq = self.one(f'SELECT count() FROM (SELECT symbol, '
                            f"date_trunc('{'month' if iv == 'm' else 'week'}', timestamp) p "
                            f'FROM "{self.table}" WHERE interval = %s '
                            f'GROUP BY symbol, p)', (iv,))
            extra = tot - uniq
            print(f"  {iv}: {tot:,} baris untuk {uniq:,} (simbol,{label}) "
                  f"= {tot/uniq:.2f}x  -> {extra:,} baris berlebih")
            self.note(f'bar {iv} berlebih', extra, f'lebih dari satu per (simbol,{label})')

        print("\n  Contoh satu simbol — mana yang dikenali API, lihat reconcile_periods.py:\n")
        rows = self.all(
            f'SELECT timestamp, open, high, low, close FROM "{self.table}" '
            f"WHERE symbol = 'BRIS.JK' AND interval = 'm' "
            f"AND timestamp >= '2026-01-01' AND timestamp < '2026-02-01' ORDER BY timestamp")
        for ts, o, h, l, cl in rows:
            mark = '  <-- awal bulan' if ts.day == 1 else ''
            print(f"    {str(ts)[:10]}  O={o} H={h} L={l} C={cl}{mark}")

        agg = self.all(
            f'SELECT first(open), max(high), min(low), last(close) FROM '
            f'(SELECT open, high, low, close FROM "{self.table}" '
            f"WHERE symbol = 'BRIS.JK' AND interval = 'd' "
            f"AND timestamp >= '2026-01-01' AND timestamp < '2026-02-01' ORDER BY timestamp)")
        if agg and agg[0][0] is not None:
            o, h, l, cl = agg[0]
            print(f"\n    agregasi harian Januari penuh: O={o} H={h} L={l} C={cl}")
            print(f"    -> baris yang cocok dengan ini adalah bar bulanan yang BENAR;")
            print(f"       sisanya jendela bergulir dan tidak boleh dipindah ke tanggal 1,")
            print(f"       karena DEDUP akan membuatnya menimpa bar yang benar.")

    # ------------------------------------------------------------------
    def dedup_health(self):
        hr("6. DUPLIKASI NYATA DI TABEL METADATA")
        print("  Versi sebelumnya menghitung 'baris berlebih' sebagai total dikurangi")
        print("  kunci unik, lalu melaporkannya sebagai cacat. Itu salah, dan alarmnya")
        print("  palsu: eodhd_stock_metadata memang menyimpan SATU baris per")
        print("  (simbol, interval) PER HARI koleksi — riwayat kesegaran, bukan tabel")
        print("  kunci-nilai. Metrik itu naik tiap hari selamanya tanpa ada yang rusak.\n")
        print("  Uji yang benar: adakah LEBIH DARI SATU baris untuk kunci yang sama")
        print("  pada hari yang sama? Itu barulah penulis yang gagal memperbarui.\n")

        rows = self.all("SELECT table_name, designatedTimestamp, dedup FROM tables()")
        no_dedup = [(n, t) for n, t, d in sorted(rows) if not d]
        if no_dedup:
            print("  Tabel tanpa DEDUP (bukan cacat dengan sendirinya):")
            for name, ts_col in no_dedup:
                print(f"    {name:28s} designated ts = {ts_col}")

        # eodhd_stock_metadata: one row per (symbol, interval) per day is correct
        tot = self.one('SELECT count() FROM eodhd_stock_metadata')
        per_day = self.one(
            'SELECT count() FROM (SELECT symbol, interval, '
            "date_trunc('day', last_updated) d FROM eodhd_stock_metadata "
            'GROUP BY symbol, interval, d)')
        dupes = (tot - per_day) if tot and per_day else 0
        print(f"\n  eodhd_stock_metadata: {tot:,} baris, {per_day:,} "
              f"(simbol,interval,hari) unik -> {dupes:,} duplikat sejati")
        if dupes:
            self.note('duplikat metadata', dupes, 'lebih dari satu baris per kunci per hari')

        # eodhd_metadata: keyed by symbol alone, so any repeat is a duplicate
        tot2 = self.one('SELECT count() FROM eodhd_metadata')
        uniq2 = self.one('SELECT count() FROM (SELECT symbol FROM eodhd_metadata '
                         'GROUP BY symbol)')
        dupes2 = (tot2 - uniq2) if tot2 and uniq2 else 0
        print(f"  eodhd_metadata      : {tot2:,} baris, {uniq2:,} simbol unik "
              f"-> {dupes2:,} duplikat sejati")
        if dupes2:
            self.note('duplikat metadata', dupes2, 'lebih dari satu baris per simbol')

        if tot and per_day:
            days = self.one("SELECT count() FROM (SELECT date_trunc('day', last_updated) d "
                            "FROM eodhd_stock_metadata GROUP BY d)")
            if days:
                print(f"\n  pertumbuhan: ~{tot // days:,} baris per hari koleksi "
                      f"(~{tot // days * 250:,}/tahun). Terpantau, belum perlu tindakan.")

    # ------------------------------------------------------------------
    def provenance(self):
        hr("7. JEJAK ASAL — bisakah kerusakan ditanggali?")
        tot = self.one(f'SELECT count() FROM "{self.table}"')
        nul = self.one(f'SELECT count() FROM "{self.table}" WHERE created_at IS NULL')
        print(f"  created_at NULL: {nul:,} / {tot:,} ({nul/tot*100:.1f}%)")
        print(f"  -> untuk mayoritas baris, waktu penulisan tidak terekam sama sekali.")
        for src, n in sorted(self.all(f'SELECT source, count() FROM "{self.table}" GROUP BY source'),
                             key=lambda r: -r[1]):
            print(f"     source={str(src):12s} {n:>12,}")
        epoch = self.one(f'SELECT count() FROM "{self.table}" WHERE year(timestamp) = 1970')
        future = self.one(f'SELECT count() FROM "{self.table}" '
                          f'WHERE year(timestamp) > {datetime.now().year + 1}')
        print(f"\n  timestamp epoch-0 (1970): {epoch:,}")
        print(f"  timestamp masa depan    : {future:,}")
        if epoch:
            self.note('timestamp epoch-0', epoch, 'tahun 1970')
        if future:
            self.note('timestamp masa depan', future, 'melewati tahun depan')

    # ------------------------------------------------------------------
    def summary(self):
        hr("RINGKASAN KELAS ANOMALI")
        if not self.findings:
            print("  tidak ada anomali terdeteksi")
            return
        width = max(len(c) for c, _, _ in self.findings)
        for cls, n, detail in sorted(self.findings, key=lambda f: -f[1]):
            print(f"  {cls:<{width}}  {n:>10,}  {detail}")
        print(f"\n  Total baris tersentuh anomali: "
              f"{sum(n for _, n, _ in self.findings):,}")
        print("\n  TIDAK ADA DATA YANG DIUBAH OLEH SKRIP INI.")

    def run(self):
        print(f"AUDIT BACA-SAJA — {self.table} — {datetime.now():%Y-%m-%d %H:%M}")
        ivs = self.intervals()
        self.null_rows(ivs)
        self.sentinel_prices(ivs)
        self.inflated_history()
        self.hour_profile(ivs)
        self.shift_probe()
        self.period_alignment()
        self.dedup_health()
        self.provenance()
        self.summary()
        self.conn.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--table', default='eodhd_stock_data')
    Audit(p.parse_args().table).run()


if __name__ == "__main__":
    main()
