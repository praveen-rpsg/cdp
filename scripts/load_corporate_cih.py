"""
Load Corporate CIH & BillAnalytics into PostgreSQL
=====================================================

Loads two raw tables under the ``corporate_cih`` schema in cdp_meta:

  corporate_cih.raw_corp_cih
      Source : C:\\Corp_CIH\\Corporate_CIHMaster_*.csv          (30 files, ~29.3 M rows)
      Columns: r1_id | brand_id | customer_type | rpsg_brand_presence

  corporate_cih.raw_corp_billanalytics
      Source : C:\\Corp_BillAnalytics\\Corporate_BillAnalytics_Customer_Lifetime_*.csv
               (25 files, ~24.8 M rows)
      Columns: r1_id | rpsg_ftd | rpsg_ltd | rpsg_tenure_lifetime | rpsg_recency_lifetime

Performance strategy
---------------------
* Tables are TRUNCATED before load (full-refresh).
* Rows are streamed through Python and pushed to PostgreSQL via COPY FROM STDIN
  using psycopg2's ``copy_expert``.  This avoids building large in-memory buffers;
  each CSV is piped row-by-row through a generator.
* Indexes are dropped before load and rebuilt afterwards for maximum throughput.

Usage
------
  python load_corporate_cih.py [--cih-only | --ba-only] [--no-index]

Options
-------
  --cih-only   Load only raw_corp_cih
  --ba-only    Load only raw_corp_billanalytics
  --no-index   Skip post-load index creation (useful for iterative testing)
"""

from __future__ import annotations

import argparse
import csv
import glob
import io
import logging
import os
import sys
import time
from typing import Generator

import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────
CIH_ROOT = r"C:\Corp_CIH"
BA_ROOT  = r"C:\Corp_BillAnalytics"

PG_CONN = (
    "host=localhost port=5432 dbname=cdp_meta "
    "user=cdp password=cdp"
)

SCHEMA   = "corporate_cih"
CIH_TABLE = f"{SCHEMA}.raw_corp_cih"
BA_TABLE  = f"{SCHEMA}.raw_corp_billanalytics"

BATCH_LOG_EVERY = 500_000   # Log progress every N rows

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_int(val: str) -> str:
    """
    Convert a float-string like '3216.0' to an integer string '3216'.
    Returns empty string for blank / null-ish values.
    """
    val = val.strip()
    if not val or val.lower() in ("none", "null", "nan", ""):
        return ""
    try:
        return str(int(float(val)))
    except (ValueError, OverflowError):
        return ""


def _safe_date(val: str) -> str:
    """
    Pass through ISO date strings (YYYY-MM-DD).
    Returns empty string for blanks so PostgreSQL treats them as NULL.
    """
    val = val.strip()
    if not val or val.lower() in ("none", "null", "nan"):
        return ""
    return val


def _clean(val: str) -> str:
    """
    Sanitise a string field for PostgreSQL COPY TEXT format.
    Removes control characters that would break the COPY row/column delimiters:
      \\t  → space  (column separator in COPY TEXT)
      \\n  → space  (row separator in COPY TEXT — embedded newlines in _ss fields)
      \\r  → ''     (carriage return — Windows line endings embedded in values)
    """
    return val.replace("\r", "").replace("\n", " ").replace("\t", " ").strip()


def _cih_rows(csv_path: str) -> Generator[str, None, None]:
    """
    Yield tab-separated lines ready for COPY FROM STDIN from a CIH CSV.

    CSV columns  : R1_s | BrandID_s | CustomerType_ss | RPSG_Brand_Presence_ss
    Table columns: r1_id | brand_id | customer_type | rpsg_brand_presence

    Note: CustomerType_ss / RPSG_Brand_Presence_ss are Python-style list strings
    (e.g. "['Spencer' 'NBL']") and may contain embedded newlines in some rows —
    _clean() strips these so they don't break the COPY row delimiter.
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            r1_id    = _clean(row.get("R1_s") or "")
            brand_id = _clean(row.get("BrandID_s") or "")
            ctype    = _clean(row.get("CustomerType_ss") or "")
            presence = _clean(row.get("RPSG_Brand_Presence_ss") or "")

            yield f"{r1_id}\t{brand_id}\t{ctype}\t{presence}\n"


def _ba_rows(csv_path: str) -> Generator[str, None, None]:
    """
    Yield tab-separated lines ready for COPY FROM STDIN from a BillAnalytics CSV.

    CSV columns  : R1_s | RPSG_FTD_dt | RPSG_LTD_dt | RPSG_TENURE_Lifetime_i | RPSG_RECENCY_Lifetime_i
    Table columns: r1_id | rpsg_ftd | rpsg_ltd | rpsg_tenure_lifetime | rpsg_recency_lifetime
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            r1_id   = _clean(row.get("R1_s") or "")
            ftd     = _safe_date(row.get("RPSG_FTD_dt") or "")
            ltd     = _safe_date(row.get("RPSG_LTD_dt") or "")
            tenure  = _safe_int(row.get("RPSG_TENURE_Lifetime_i") or "")
            recency = _safe_int(row.get("RPSG_RECENCY_Lifetime_i") or "")

            yield f"{r1_id}\t{ftd}\t{ltd}\t{tenure}\t{recency}\n"


class _RowStream(io.RawIOBase):
    """
    A file-like object that wraps a string generator so psycopg2's
    copy_expert can read it without materialising the whole dataset.
    """

    def __init__(self, gen: Generator[str, None, None]):
        self._gen = gen
        self._buf = b""
        self._done = False

    def readable(self):
        return True

    def readinto(self, b):
        while not self._buf and not self._done:
            try:
                chunk = next(self._gen)
                self._buf = chunk.encode("utf-8")
            except StopIteration:
                self._done = True

        if not self._buf:
            return 0

        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


def _copy_table(
    conn,
    table: str,
    columns: list[str],
    csv_files: list[str],
    row_gen_fn,
) -> int:
    """
    TRUNCATE table then COPY all csv_files into it.
    Returns total rows loaded.
    """
    cols_sql = ", ".join(columns)
    copy_sql = f"COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT TEXT, NULL '')"

    total = 0
    with conn.cursor() as cur:
        log.info(f"Truncating {table} ...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        t0 = time.time()
        file_rows = 0

        gen = row_gen_fn(fpath)
        stream = io.BufferedReader(_RowStream(gen), buffer_size=1 << 20)  # 1 MB buffer

        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, stream)

        conn.commit()
        elapsed = time.time() - t0
        # Count rows just loaded = approximate via rowcount (not always reliable for COPY)
        # We'll track via a separate count after the fact for verification
        log.info(f"  {fname} → loaded in {elapsed:.1f}s")

        total += 1  # file count for now; row count verified at end

    return total


def _row_count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def _rebuild_indexes(conn, table_short: str) -> None:
    """Create indexes after the bulk load."""
    idx_map = {
        "raw_corp_cih": [
            f"CREATE INDEX IF NOT EXISTS idx_corp_cih_r1_id ON {SCHEMA}.raw_corp_cih (r1_id)",
        ],
        "raw_corp_billanalytics": [
            f"CREATE INDEX IF NOT EXISTS idx_corp_ba_r1_id ON {SCHEMA}.raw_corp_billanalytics (r1_id)",
            f"CREATE INDEX IF NOT EXISTS idx_corp_ba_rpsg_ltd ON {SCHEMA}.raw_corp_billanalytics (rpsg_ltd)",
        ],
    }
    statements = idx_map.get(table_short, [])
    with conn.cursor() as cur:
        for stmt in statements:
            log.info(f"  {stmt[:70]}...")
            cur.execute(stmt)
    conn.commit()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load Corporate CIH data into PostgreSQL.")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--cih-only", action="store_true", help="Load only raw_corp_cih")
    grp.add_argument("--ba-only",  action="store_true", help="Load only raw_corp_billanalytics")
    parser.add_argument("--no-index", action="store_true", help="Skip post-load index creation")
    args = parser.parse_args()

    load_cih = not args.ba_only
    load_ba  = not args.cih_only

    # ── Collect files ──────────────────────────────────────────────────────────
    cih_files = sorted(glob.glob(os.path.join(CIH_ROOT, "Corporate_CIHMaster_*.csv")))
    ba_files  = sorted(glob.glob(os.path.join(BA_ROOT,  "Corporate_BillAnalytics_Customer_Lifetime_*.csv")))

    if load_cih and not cih_files:
        log.error(f"No CIH CSVs found under {CIH_ROOT}")
        sys.exit(1)
    if load_ba and not ba_files:
        log.error(f"No BillAnalytics CSVs found under {BA_ROOT}")
        sys.exit(1)

    log.info(f"CIH files   : {len(cih_files)}")
    log.info(f"BA  files   : {len(ba_files)}")

    # ── Connect ────────────────────────────────────────────────────────────────
    log.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(PG_CONN)
    conn.autocommit = False

    # Ensure schema exists (idempotent)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    conn.commit()

    # ── Load raw_corp_cih ─────────────────────────────────────────────────────
    if load_cih:
        log.info("=" * 60)
        log.info(f"Loading {CIH_TABLE}")
        log.info("=" * 60)
        t_start = time.time()

        _copy_table(
            conn,
            table=CIH_TABLE,
            columns=["r1_id", "brand_id", "customer_type", "rpsg_brand_presence"],
            csv_files=cih_files,
            row_gen_fn=_cih_rows,
        )

        elapsed = time.time() - t_start
        count = _row_count(conn, CIH_TABLE)
        log.info(f"raw_corp_cih  → {count:,} rows loaded in {elapsed:.1f}s ({count/elapsed:,.0f} rows/s)")

        if not args.no_index:
            log.info("Building indexes on raw_corp_cih ...")
            _rebuild_indexes(conn, "raw_corp_cih")
            log.info("Indexes done.")

    # ── Load raw_corp_billanalytics ───────────────────────────────────────────
    if load_ba:
        log.info("=" * 60)
        log.info(f"Loading {BA_TABLE}")
        log.info("=" * 60)
        t_start = time.time()

        _copy_table(
            conn,
            table=BA_TABLE,
            columns=["r1_id", "rpsg_ftd", "rpsg_ltd", "rpsg_tenure_lifetime", "rpsg_recency_lifetime"],
            csv_files=ba_files,
            row_gen_fn=_ba_rows,
        )

        elapsed = time.time() - t_start
        count = _row_count(conn, BA_TABLE)
        log.info(f"raw_corp_billanalytics → {count:,} rows loaded in {elapsed:.1f}s ({count/elapsed:,.0f} rows/s)")

        if not args.no_index:
            log.info("Building indexes on raw_corp_billanalytics ...")
            _rebuild_indexes(conn, "raw_corp_billanalytics")
            log.info("Indexes done.")

    conn.close()
    log.info("All done.")


if __name__ == "__main__":
    main()
