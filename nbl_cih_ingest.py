"""
NBL CIH Data Ingestion Script
Loads NBL_CIHMaster_1.csv, NBL_CIHMaster_2.csv, NBL_CIHMaster_3.csv
into nb_bronze.raw_cih_profiles with column mapping.

CSV columns differ from table columns; this script maps them explicitly.
"""

import csv
import os
import re
import sys
from datetime import datetime, timezone

import psycopg2

DB_DSN = "host=localhost port=5432 dbname=cdp_meta user=cdp password=cdp"
CIH_DIR = r"C:\NBL"
CIH_FILES = [
    "NBL_CIHMaster_1.csv",
    "NBL_CIHMaster_2.csv",
    "NBL_CIHMaster_3.csv",
]
TARGET_TABLE = "nb_bronze.raw_cih_profiles"

# Mapping: CSV column header → target table column name
# CSV columns not listed here are silently ignored.
CSV_TO_TABLE = {
    "BrandID_s":              "brand_id",
    "R1_s":                   "r1",
    "Name_s":                 "name",
    "City_s":                 "city",
    "Created_At_dt":          "created_at",
    "Customer_Group_s":       "customer_group",
    "Device_History_s":       "device_history",
    "DOB_dt":                 "dob",
    "dob_day_i":              "dob_day",
    "dob_month_i":            "dob_month",
    "dob_year_i":             "dob_year",
    "Email_domain":           "email_domain",
    "Last_Device_s":          "last_device",
    "Occupation_s":           "occupation",
    "Pincode_s":              "pincode",
    "Status_s":               "status",
    "StoreCode_s":            "store_code",
    "Street_s":               "street",
    "Region_s":               "region",
    "Employee_s":             "employee",
    "WhatsApp_s":             "whatsapp",
    "Age_i":                  "age",
    "DND_s":                  "dnd",
    "First_Name_s":           "first_name",
    "Last_Name_s":            "last_name",
    "Accepts_Email_Marketing_s": "accepts_email_marketing",
    "Accepts_SMS_Marketing_s":   "accepts_sms_marketing",
    "Total_Orders_i":         "total_orders",
    "Total_Spent_d":          "total_spent",
    "Company_s":              "company",
    "Country_s":              "country",
    "EmailID_s":              "email_id",
    "Alternate_Street_1_s":   "alternate_street_1",
    "Alternate_Street_2_s":   "alternate_street_2",
    "Alternate_Street_3_s":   "alternate_street_3",
    "Alternate_Street_4_s":   "alternate_street_4",
    "Alternate_Street_5_s":   "alternate_street_5",
    "Alternate_City_1_s":     "alternate_city_1",
    "Alternate_City_2_s":     "alternate_city_2",
    "Alternate_City_3_s":     "alternate_city_3",
    "Alternate_City_4_s":     "alternate_city_4",
    "Alternate_City_5_s":     "alternate_city_5",
    "Alternate_Region_1_s":   "alternate_region_1",
    "Alternate_Region_2_s":   "alternate_region_2",
    "Alternate_Region_3_s":   "alternate_region_3",
    "Alternate_Region_4_s":   "alternate_region_4",
    "Alternate_Region_5_s":   "alternate_region_5",
    "Alternate_Pincode_1_s":  "alternate_pincode_1",
    "Alternate_Pincode_2_s":  "alternate_pincode_2",
    "Alternate_Pincode_3_s":  "alternate_pincode_3",
    "Alternate_Pincode_4_s":  "alternate_pincode_4",
    "Alternate_Pincode_5_s":  "alternate_pincode_5",
}

# Table columns that exist but have no corresponding CSV column (will be NULL)
NULL_COLS = [
    "pd_preferred_store",
    "pd_subscription_end_date",
    "pd_subscription_renewal_count",
    "pd_store_contact_1",
    "pd_store_contact_2",
    "gw_customer_flag",
    "customer_surrogate_id",
]

TABLE_COLS = list(CSV_TO_TABLE.values()) + NULL_COLS + ["_loaded_at", "_source_file"]


def null_if_empty(val):
    """Return None for empty or whitespace-only strings."""
    if val is None:
        return None
    stripped = val.strip()
    return stripped if stripped else None


def ingest_file(cur, filepath: str, loaded_at: datetime) -> int:
    filename = os.path.basename(filepath)
    print(f"  Loading {filename} ...", flush=True)

    col_names = TABLE_COLS
    placeholders = ", ".join(["%s"] * len(col_names))
    col_list = ", ".join(col_names)
    insert_sql = (
        f"INSERT INTO {TARGET_TABLE} ({col_list}) VALUES ({placeholders})"
    )

    rows_loaded = 0
    with open(filepath, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        batch = []

        for row in reader:
            record = []
            for csv_col, tbl_col in CSV_TO_TABLE.items():
                record.append(null_if_empty(row.get(csv_col)))

            # Null columns
            for _ in NULL_COLS:
                record.append(None)

            # Metadata
            record.append(loaded_at)
            record.append(filename)

            batch.append(tuple(record))

            if len(batch) >= 5000:
                cur.executemany(insert_sql, batch)
                rows_loaded += len(batch)
                batch = []

        if batch:
            cur.executemany(insert_sql, batch)
            rows_loaded += len(batch)

    print(f"    -> {rows_loaded:,} rows inserted", flush=True)
    return rows_loaded


def main():
    print(f"Connecting to {DB_DSN.split('password')[0]}...", flush=True)
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    cur = conn.cursor()

    loaded_at = datetime.now(tz=timezone.utc)
    total = 0

    try:
        for fname in CIH_FILES:
            fpath = os.path.join(CIH_DIR, fname)
            if not os.path.exists(fpath):
                print(f"  WARNING: {fpath} not found — skipping", flush=True)
                continue
            total += ingest_file(cur, fpath, loaded_at)

        conn.commit()
        print(f"\nDone. Total rows inserted: {total:,}", flush=True)

    except Exception as exc:
        conn.rollback()
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
