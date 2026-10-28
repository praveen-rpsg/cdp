#!/usr/bin/env python3
"""
CESC Bronze ingestion: CIH master + Lifetime attributes.

Target file location in the project: dwh/ingestion/ingest_cesc.py
(sits alongside dwh/sql/07_cesc_bronze_tables.sql, which is the authoritative
schema this script loads into).

SCOPE (current run)
--------------------
  * bronze.cesc_cih_master           <- D:\\CIH_Master\\CESC_{1,2,3}.csv
  * bronze.cesc_lifetime_attributes  <- D:\\BillAnalytics_Customer_Lifetime\\
                                         CESC_BillAnalytics_Customer_Lifetime_{1,2,3}.csv

  bronze.cesc_bill_monthly is wired for the verified February 2026 TSV dump.

SAFETY / BRONZE RULES ENFORCED HERE
------------------------------------
  * No DROP / TRUNCATE / DELETE of any CESC table, ever.
  * No CREATE TABLE / DDL execution -- 07_cesc_bronze_tables.sql is assumed
    to already exist in the target database (per the task brief).
  * Source CSVs are opened read-only and never modified.
  * r1_s is loaded as plain source text. This script performs no identity
    resolution and never treats r1_s (or any other field) as a UNIFY360 key.
  * CIH _ss array fields are preserved as parsed source field values as they appear in the CSV
    field, embedded newlines included. They are never split, reflowed, or
    "cleaned" -- see PARSING NOTES below.
  * Every load is additive (COPY only). Re-ingesting a file with changed
    content lands new rows alongside the old ones; nothing is deleted, so
    every previous snapshot is preserved.

INCREMENTAL / IDEMPOTENCY DESIGN
---------------------------------
  For each source file:
    1. Compute a streaming SHA-256 of the file's raw bytes.
    2. Look in bronze.cesc_ingestion_log for a prior row with the same
       (dataset_name, source_file basename, extra->>'file_sha256') where
       status = 'SUCCESS'. If found, skip the file entirely (log + continue).
    3. Otherwise load it. Same filename + different hash is therefore
       automatically treated as a new version and re-ingested -- old rows
       from the earlier version are left untouched in the bronze table.
    4. Record exactly one log row per file per run in
       bronze.cesc_ingestion_log, sharing an ingestion_batch_id (one fresh
       UUID per file) with that file's rows in the data table's
       _ingestion_batch_id column. On failure, the data-table insert is
       rolled back but the FAILED log row is still committed, so failures
       are never silently lost.

PARSING NOTES (why pandas.read_csv, not a line reader)
--------------------------------------------------------
  CIH _ss fields such as Bill_Ac_Month_ss are delivered as a single quoted
  CSV field whose value is a Python-list-looking string, e.g.:

      "['202207' '202208' '202209' ...
       '202303' '202304' ...]"

  

REQUIREMENTS
------------
  pip install pandas "psycopg[binary]" python-dotenv

CONFIGURATION
-------------
  DATABASE_URL   postgresql://cdp:<password>@localhost:5432/cdp_meta
                 (set in a .env file next to this script, or in the
                 environment; there is a same-shaped default matching the
                 project's convention, but it does NOT include a real
                 password -- set DATABASE_URL explicitly before running)
  CESC_CIH_DIR       default D:\\CIH_Master
  CESC_LIFETIME_DIR  default D:\\BillAnalytics_Customer_Lifetime

"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

load_dotenv()

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://cdp:cdp@localhost:5432/cdp_meta",
)

CHUNK_SIZE = 50_000          # rows per pandas chunk (streaming, not full-file load)
HASH_BUF_SIZE = 1024 * 1024  # bytes per read() while hashing
PROGRESS_EVERY = 100_000     # log a progress line every N rows written

DEFAULT_CIH_DIR = Path(os.getenv("CESC_CIH_DIR", r"D:\CIH_Master"))
DEFAULT_LIFETIME_DIR = Path(
    os.getenv("CESC_LIFETIME_DIR", r"D:\BillAnalytics_Customer_Lifetime")
)

CIH_FILES = ["CESC_1.csv", "CESC_2.csv", "CESC_3.csv"]
LIFETIME_FILES = [
    "CESC_BillAnalytics_Customer_Lifetime_1.csv",
    "CESC_BillAnalytics_Customer_Lifetime_2.csv",
    "CESC_BillAnalytics_Customer_Lifetime_3.csv",
]

DEFAULT_BILL_DIR = Path(
    os.getenv("CESC_BILL_DIR", r"D:\cdp_data_202602\cdp_data_202602")
)
BILL_PATTERN = "cdp_bill_data_202602_*.tsv"
EXPECTED_BILL_MONTH = "202602"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("cesc_ingest")

# ----------------------------------------------------------------------------
# Explicit source-header -> bronze-column mappings.
#
# These are POSITIONAL and are validated against the real file header at
# read time (see ingest_file). They are written out explicitly rather than
# derived by lower-casing the source header, because several source columns
# do not lower-case cleanly onto their DB name (e.g. HouseNo_s -> house_no_s,
# not houseno_s; Conumer_status_Code_ss -> consumer_status_code_ss, which
# also fixes the source typo per 07_cesc_bronze_tables.sql).
# ----------------------------------------------------------------------------

CIH_COLUMNS = [
    ("R1_s", "r1_s"),
    ("Name_s", "name_s"),
    ("HouseNo_s", "house_no_s"),
    ("Floor_s", "floor_s"),
    ("Street_s", "street_s"),
    ("City_s", "city_s"),
    ("Pincode_s", "pincode_s"),
    ("EmailID_s", "email_id_s"),
    ("BrandID_s", "brand_id_s"),
    ("IsMobile_b", "is_mobile_b"),
    ("MobileNo_s", "mobile_no_s"),
    ("Email_domain_s", "email_domain_s"),
    ("DND_s", "dnd_s"),
    ("Mode_Of_Payment_s", "mode_of_payment_s"),
    ("Payment_Mode_Description_s", "payment_mode_description_s"),
    ("Bill_Ac_Month_ss", "bill_ac_month_ss"),
    ("Consumer_No_ss", "consumer_no_ss"),
    ("Regional_Office_ss", "regional_office_ss"),
    ("Consumer_Type_ss", "consumer_type_ss"),
    ("Road_Code_ss", "road_code_ss"),
    ("Premises_No_ss", "premises_no_ss"),
    ("Lamp_Post_No_ss", "lamp_post_no_ss"),
    ("Event_Bill_Ac_Month_ss", "event_bill_ac_month_ss"),
    ("Bill_Type_ss", "bill_type_ss"),
    ("Consumer_Account_status_ss", "consumer_account_status_ss"),
    ("Ebill_Status_ss", "ebill_status_ss"),
    ("Supply_Mode_ss", "supply_mode_ss"),
    ("Supply_Type_ss", "supply_type_ss"),
    ("Conumer_status_Code_ss", "consumer_status_code_ss"),
]

LIFETIME_COLUMNS = [
    ("R1_s", "r1_s"),
    ("FBD_Lifetime_dt", "fbd_lifetime_dt"),
    ("SBD_Lifetime_dt", "sbd_lifetime_dt"),
    ("LBD_Lifetime_dt", "lbd_lifetime_dt"),
    ("TENURE_Lifetime_i", "tenure_lifetime_i"),
    ("BILLS_Lifetime_i", "bills_lifetime_i"),
    ("SPEND_Lifetime_d", "spend_lifetime_d"),
    ("SPENDPERBILL_Lifetime_d", "spendperbill_lifetime_d"),
    ("RECENCY_Lifetime_i", "recency_lifetime_i"),
    ("TotalDiscount_Lifetime_d", "totaldiscount_lifetime_d"),
]

BILL_COLUMNS = [
    ("CUSTOMER ID.", "customer_id"),
    ("BILL A/C MONTH", "bill_ac_month"),
    ("CONSUMER NO.", "consumer_no"),
    ("CHECK DIGIT (11TH DIGIT OF CONSUMER NO)", "consumer_check_digit"),
    ("REGIONAL OFFICE", "regional_office"),
    ("ADDRESS_LINE ONE (NAME)", "address_line_one"),
    ("ADDRESS_LINE TWO", "address_line_two"),
    ("ADDRESS_LINE THREE", "address_line_three"),
    ("ADDRESS_LINE FOUR", "address_line_four"),
    ("ADDRESS_LINE FIVE", "address_line_five"),
    ("ADDRESS_LINE SIX", "address_line_six"),
    ("SUPPLY ADDRESS", "supply_address"),
    ("NET AMOUNT (RS.)", "net_amount"),
    ("REBATE AMOUNT (RS.)", "rebate_amount"),
    ("GROSS AMOUNT (RS.)", "gross_amount"),
    ("DUE DATE", "due_date"),
    ("ARREAR AMOUNT", "arrear_amount"),
    ("ADJUSTMENT AMOUNT", "adjustment_amount"),
    ("GOVT. DUTY (GD)", "govt_duty"),
    ("METER RENT AMOUNT", "meter_rent_amount"),
    ("SECURITY DEPOSITE AMOUNT", "security_deposit_amount"),
    ("ENERGY CHARGES AMOUNT", "energy_charges_amount"),
    ("TOTAL UNIT", "total_unit"),
    ("LOAD (KW)", "load_kw"),
    ("CONSUMER TYPE", "consumer_type"),
    ("SUPPLY TYPE", "supply_type"),
    ("ROAD CODE", "road_code"),
    ("PREMISES NO", "premises_no"),
    ("LAMP POST NO", "lamp_post_no"),
    ("SUPPLY MODE", "supply_mode"),
    ("TAX STATUS", "tax_status"),
    ("VOLTAGE STATUS", "voltage_status"),
    ("CONUMER STATUS CODE", "consumer_status_code"),
    ("AVG. UNIT", "avg_unit"),
    ("CONSUMER ACCOUNT STATUS", "consumer_account_status"),
    ("EVENT BILL A/C MONTH", "event_bill_ac_month"),
    ("EVENT DATE", "event_date"),
    ("EVENT TYPE", "event_type"),
    ("BILL TYPE", "bill_type"),
    ("FIXED DEMAND CHARGES", "fixed_demand_charges"),
    ("SUBSIDY STATUS", "subsidy_status"),
    ("ADVANCE PAYMENT STATUS", "advance_payment_status"),
    ("ADVANCE PAYMENT ADJUSTMENT AMOUNT", "advance_payment_adjustment_amount"),
    ("MONTHLY VARIABLE COST ADJUSTMENT AMOUNT", "monthly_variable_cost_adj_amount"),
    ("PUBLIC BODY STATUS CODE", "public_body_status_code"),
    ("BILL DATE", "bill_date"),
    ("EBILL STATUS", "ebill_status"),
    ("EMAIL ID", "email_id"),
    ("MOBILE NO", "mobile_no"),
    ("PIN_CD", "pin_cd"),
    ("MODE_OF_PMNT_PROCESSED", "mode_of_pmnt_processed"),
    ("PMNT_MODE_DESC", "pmnt_mode_desc"),
    ("FPPAS AMOUNT", "fppas_amount"),
]

# Registry of datasets this script knows how to load. To add CESC bill
# ingestion later (once the corrected Feb-2026 files land), add a
# "bill_monthly" entry here with its own column mapping / file list / table,
# and add "bill_monthly" to the --dataset choices in main(). Everything else
# (hashing, skip logic, COPY streaming, logging) is already generic.
DATASETS = {
    "cih": {
        "table": "bronze.cesc_cih_master",
        "dataset_name": "cih_master",
        "columns": CIH_COLUMNS,
        "default_dir": DEFAULT_CIH_DIR,
        "files": CIH_FILES,
        "delimiter": ",",
        "encoding": "utf-8-sig",
    },
    "lifetime": {
        "table": "bronze.cesc_lifetime_attributes",
        "dataset_name": "lifetime_attributes",
        "columns": LIFETIME_COLUMNS,
        "default_dir": DEFAULT_LIFETIME_DIR,
        "files": LIFETIME_FILES,
        "delimiter": ",",
        "encoding": "utf-8-sig",
    },
    "bill": {
        "table": "bronze.cesc_bill_monthly",
        "dataset_name": "bill_monthly",
        "columns": BILL_COLUMNS,
        "default_dir": DEFAULT_BILL_DIR,
        "pattern": BILL_PATTERN,
        "delimiter": "\t",
        "encoding": "utf-8-sig",
        "expected_month": EXPECTED_BILL_MONTH,
        "month_column": "BILL A/C MONTH",
        "month_index": 1,
    },
}


# ----------------------------------------------------------------------------
# DB helpers
# ----------------------------------------------------------------------------

def get_conn() -> psycopg.Connection:
    return psycopg.connect(DB_URL, autocommit=False)


def already_ingested(conn: psycopg.Connection, dataset_name: str,
                      source_file: str, file_hash: str) -> bool:
    """True if this exact (dataset, filename, content) combination already
    has a SUCCESS row in the ingestion log."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT log_id
            FROM bronze.cesc_ingestion_log
            WHERE dataset_name = %s
              AND source_file = %s
              AND status = 'SUCCESS'
              AND extra ->> 'file_sha256' = %s
            LIMIT 1
            """,
            (dataset_name, source_file, file_hash),
        )
        return cur.fetchone() is not None


def insert_log_row(conn: psycopg.Connection, *, batch_id: str, dataset_name: str,
                    source_file: str, started_at: datetime, finished_at: datetime,
                    status: str, rows_read, rows_loaded,
                    error_message: str | None = None,
                    error_detail: str | None = None,
                    extra: dict | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.cesc_ingestion_log (
                ingestion_batch_id, source_system, dataset_name, source_file,
                run_started_at, run_finished_at, status,
                rows_read, rows_loaded, error_message, error_detail, extra
            ) VALUES (%s, 'CESC', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                batch_id, dataset_name, source_file, started_at, finished_at,
                status, rows_read, rows_loaded, error_message, error_detail,
                json.dumps(extra) if extra is not None else None,
            ),
        )
    conn.commit()


# ----------------------------------------------------------------------------
# File helpers
# ----------------------------------------------------------------------------

def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(HASH_BUF_SIZE), b""):
            h.update(block)
    return h.hexdigest()


def clean_value(v):
    """Empty CSV fields -> SQL NULL. Everything else passes through exactly
    as read: no trimming, no case changes. This preserves source fidelity,
    including intentional padding in name/address fields and the raw
    "['202207' '202208' ...]" array text (newlines included)."""
    if v is None:
        return None
    if isinstance(v, str) and v == "":
        return None
    return v


# ----------------------------------------------------------------------------
# Core ingestion
# ----------------------------------------------------------------------------

def ingest_file(conn: psycopg.Connection, spec: dict, path: Path, batch_id: str) -> dict:
    """Stream one source file into its bronze table via COPY FROM STDIN.

    Runs inside the caller's open transaction; does NOT commit. Raises on
    any unrecoverable error (header mismatch, validation failure, bad row,
    DB error) so the caller can roll back and log a FAILED row.
    """
    table = spec["table"]
    expected_header = [src for src, _ in spec["columns"]]
    db_columns = [dst for _, dst in spec["columns"]]
    meta_columns = ["_source_file", "_source_system", "_ingestion_batch_id"]

    copy_sql = (
        f"COPY {table} ({', '.join(db_columns + meta_columns)}) FROM STDIN"
    )

    rows_read = 0
    rows_loaded = 0

    read_kwargs = {
        "dtype": str,
        "keep_default_na": False,
        "na_values": [],
        "sep": spec.get("delimiter", ","),
        "encoding": spec.get("encoding", "utf-8-sig"),
        "engine": "c",
    }

    # Validate the header independently so header-only files (e.g. Feb _01.tsv)
    # are recognized correctly.
    header_df = pd.read_csv(path, nrows=0, **read_kwargs)
    actual_header = list(header_df.columns)
    if actual_header != expected_header:
        raise ValueError(
            f"{path.name}: header does not match the verified CESC layout.\n"
            f"  expected: {expected_header}\n"
            f"  actual:   {actual_header}"
        )

    reader = pd.read_csv(
        path,
        chunksize=CHUNK_SIZE,
        **read_kwargs,
    )

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            first_chunk = True

            for chunk in reader:
                # For February monthly bills, validate the first three actual
                # data rows in every file against the expected bill month.
                expected_month = spec.get("expected_month")
                month_index = spec.get("month_index")

                if first_chunk and expected_month is not None:
                    sample = chunk.iloc[:3, month_index].tolist()
                    bad = [v for v in sample if v != expected_month]
                    if bad:
                        raise ValueError(
                            f"{path.name}: BILL A/C MONTH validation failed. "
                            f"Expected {expected_month} in first {len(sample)} "
                            f"data row(s), observed {sample}"
                        )

                first_chunk = False
                rows_read += len(chunk)

                for row in chunk.itertuples(index=False, name=None):
                    values = tuple(clean_value(v) for v in row)
                    copy.write_row(values + (path.name, "CESC", batch_id))
                    rows_loaded += 1

                    if rows_loaded % PROGRESS_EVERY == 0:
                        logger.info(
                            "  ... %s rows written (%s)",
                            rows_loaded,
                            path.name,
                        )

    return {"rows_read": rows_read, "rows_loaded": rows_loaded}


def process_dataset(conn: psycopg.Connection, dataset_key: str, source_dir: Path,
                     dry_run: bool = False, only_file: str | None = None) -> None:
    spec = DATASETS[dataset_key]
    dataset_name = spec["dataset_name"]

    if "pattern" in spec:
        paths = sorted(source_dir.glob(spec["pattern"]))
        filenames = [p.name for p in paths]
        if only_file is not None:
            filenames = [only_file]
    else:
        filenames = spec["files"] if only_file is None else [only_file]

    if not filenames:
        logger.warning("No files found for dataset=%s in %s", dataset_name, source_dir)
        return

    for filename in filenames:
        path = source_dir / filename
        if not path.exists():
            logger.warning("SKIP missing file: %s", path)
            continue

        logger.info("Hashing %s ...", path)
        file_hash = sha256_of_file(path)

        if already_ingested(conn, dataset_name, filename, file_hash):
            logger.info(
                "SKIP already ingested %s -> %s [sha256=%s...]",
                filename,
                spec["table"],
                file_hash[:12],
            )
            continue

        if dry_run:
            logger.info(
                "[DRY RUN] Would ingest %s -> %s [sha256=%s...]",
                filename,
                spec["table"],
                file_hash[:12],
            )
            continue

        batch_id = str(uuid.uuid4())
        run_started_at = datetime.now(timezone.utc)

        try:
            logger.info(
                "Ingesting %s -> %s [dataset=%s batch=%s sha256=%s...]",
                filename,
                spec["table"],
                dataset_name,
                batch_id,
                file_hash[:12],
            )
            result = ingest_file(conn, spec, path, batch_id)

            run_finished_at = datetime.now(timezone.utc)
            insert_log_row(
                conn,
                batch_id=batch_id,
                dataset_name=dataset_name,
                source_file=filename,
                started_at=run_started_at,
                finished_at=run_finished_at,
                status="SUCCESS",
                rows_read=result["rows_read"],
                rows_loaded=result["rows_loaded"],
                error_message=None,
                error_detail=None,
                extra={"file_sha256": file_hash, "source_path": str(path)},
            )

            conn.commit()

            logger.info(
                "SUCCESS %s: rows_read=%s rows_loaded=%s",
                filename,
                result["rows_read"],
                result["rows_loaded"],
            )

        except Exception as exc:
            error_detail = traceback.format_exc()
            conn.rollback()

            run_finished_at = datetime.now(timezone.utc)
            try:
                insert_log_row(
                    conn,
                    batch_id=batch_id,
                    dataset_name=dataset_name,
                    source_file=filename,
                    started_at=run_started_at,
                    finished_at=run_finished_at,
                    status="FAILED",
                    rows_read=None,
                    rows_loaded=0,
                    error_message=str(exc),
                    error_detail=error_detail,
                    extra={"file_sha256": file_hash, "source_path": str(path)},
                )
            except Exception:
                conn.rollback()
                logger.exception("Could not write FAILED log row for %s", filename)

            logger.error("FAILED %s: %s", filename, exc)


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CESC Bronze ingestion: CIH master + Lifetime attributes + monthly bills "
                     "(incremental, hash-based)."
    )
    parser.add_argument(
        "--dataset", choices=["cih", "lifetime", "bill", "all"], default="all",
        help="Which dataset(s) to process (default: all).",
    )
    parser.add_argument(
        "--cih-dir", type=Path, default=DEFAULT_CIH_DIR,
        help=f"Directory containing CIH CSVs (default: {DEFAULT_CIH_DIR}).",
    )
    parser.add_argument(
        "--lifetime-dir", type=Path, default=DEFAULT_LIFETIME_DIR,
        help=f"Directory containing Lifetime CSVs (default: {DEFAULT_LIFETIME_DIR}).",
    )
    parser.add_argument(
        "--bill-dir", type=Path, default=DEFAULT_BILL_DIR,
        help=f"Directory containing February 2026 bill TSVs (default: {DEFAULT_BILL_DIR}).",
    )
    parser.add_argument(
        "--only-file", default=None,
        help="Restrict the run to a single filename within the chosen "
             "dataset's directory (useful for a first small test).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Hash files and report what would be loaded/skipped; no DB writes.",
    )
    args = parser.parse_args()

    if args.only_file and args.dataset == "all":
        parser.error("--only-file requires --dataset cih, lifetime, or bill")

    conn = get_conn()
    try:
        if args.dataset in ("cih", "all"):
            process_dataset(
                conn, "cih", args.cih_dir,
                dry_run=args.dry_run,
                only_file=args.only_file if args.dataset == "cih" else None,
            )
        if args.dataset in ("lifetime", "all"):
            process_dataset(
                conn, "lifetime", args.lifetime_dir,
                dry_run=args.dry_run,
                only_file=args.only_file if args.dataset == "lifetime" else None,
            )
        if args.dataset in ("bill", "all"):
            process_dataset(
                conn, "bill", args.bill_dir,
                dry_run=args.dry_run,
                only_file=args.only_file if args.dataset == "bill" else None,
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()


# ----------------------------------------------------------------------------
# BILL INGESTION NOTES
# ----------------------------------------------------------------------------
# February 2026 files are tab-delimited TSVs in DEFAULT_BILL_DIR.
# The loader discovers only files matching BILL_PATTERN (present files only),
# validates the 53-column header, validates 202602 on the first three data rows
# of each file, and uses the same hash-based idempotency / transactional COPY
# path as CIH and Lifetime.
# ----------------------------------------------------------------------------
