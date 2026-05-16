"""
Load NBL Location Master into nb_bronze.raw_location_master
=============================================================

Reads all Location_Master CSVs from the 11 daily folders under C:/NBL,
de-duplicates on store_code (latest file wins), truncates the table,
and bulk-inserts the unique store records.

The CSVs are pipe-delimited with a single header row.

Column mapping  CSV → PostgreSQL:
  STORECODE           → store_code
  STORENAME           → store_name
  STOREFORMAT         → store_format
  STOREZONE           → store_zone
  STOREBUSINESSREGION → store_business_region
  STOREREGIONCODE     → store_region_code
  STORESTATE          → store_state
  STORECITYCODE       → store_city_code
  STORECITYDESCRIPTION→ store_city_description
  STOREPINCODE        → store_pincode
  STOREADDRESS        -> store_address
  STATUS              → status
  STOREOPENINGDATE    → store_opening_date
  STORECLOSINGDATE    → store_closing_date
"""

import csv
import glob
import os
import sys

import psycopg2

# ── Config ────────────────────────────────────────────────────────────────────
NBL_ROOT   = r"C:\NBL"
PG_CONN    = "host=localhost port=5432 dbname=cdp_meta user=cdp password=cdp"
TARGET     = "nb_bronze.raw_location_master"

COL_MAP = [
    ("STORECODE",            "store_code"),
    ("STORENAME",            "store_name"),
    ("STOREFORMAT",          "store_format"),
    ("STOREZONE",            "store_zone"),
    ("STOREBUSINESSREGION",  "store_business_region"),
    ("STOREREGIONCODE",      "store_region_code"),
    ("STORESTATE",           "store_state"),
    ("STORECITYCODE",        "store_city_code"),
    ("STORECITYDESCRIPTION", "store_city_description"),
    ("STOREPINCODE",         "store_pincode"),
    ("STOREADDRESS",         "store_address"),
    ("STATUS",               "status"),
    ("STOREOPENINGDATE",     "store_opening_date"),
    ("STORECLOSINGDATE",     "store_closing_date"),
]
CSV_COLS = [c for c, _ in COL_MAP]
PG_COLS  = [p for _, p in COL_MAP]

# ── Collect all Location_Master CSVs sorted oldest→newest ─────────────────────
pattern = os.path.join(NBL_ROOT, "*", "Location_Master_*.csv")
csv_files = sorted(glob.glob(pattern))          # sort by folder date (oldest first)

if not csv_files:
    print(f"ERROR: No Location_Master CSV files found under {NBL_ROOT}")
    sys.exit(1)

print(f"Found {len(csv_files)} Location_Master file(s):")
for f in csv_files:
    print(f"  {f}")

# ── Read all rows, de-dup on store_code (later file overwrites earlier) ────────
stores: dict[str, dict] = {}   # store_code → row dict

for fpath in csv_files:
    with open(fpath, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="|")
        rows_in_file = 0
        for row in reader:
            # Strip BOM / whitespace from all values
            cleaned = {k.strip(): (v.strip() if v else "") for k, v in row.items()}
            code = cleaned.get("STORECODE", "").strip()
            if not code:
                continue
            stores[code] = {pg_col: cleaned.get(csv_col, "") for csv_col, pg_col in COL_MAP}
            rows_in_file += 1
        print(f"  >> {os.path.basename(fpath)}: {rows_in_file} rows read")

print(f"\nUnique store codes after de-dup: {len(stores)}")

if not stores:
    print("ERROR: No valid rows found. Aborting.")
    sys.exit(1)

# ── Connect and load ──────────────────────────────────────────────────────────
print(f"\nConnecting to PostgreSQL: {PG_CONN.replace('password=cdp', 'password=***')}")
conn = psycopg2.connect(PG_CONN)
conn.autocommit = False
cur = conn.cursor()

try:
    # Truncate first — location master is a full-refresh table
    print(f"Truncating {TARGET} ...")
    cur.execute(f"TRUNCATE TABLE {TARGET}")

    # Bulk insert with executemany
    placeholders = ", ".join(["%s"] * len(PG_COLS))
    insert_sql = (
        f"INSERT INTO {TARGET} ({', '.join(PG_COLS)}) "
        f"VALUES ({placeholders})"
    )

    rows_to_insert = [
        tuple(row[col] for col in PG_COLS)
        for row in stores.values()
    ]

    print(f"Inserting {len(rows_to_insert)} rows into {TARGET} ...")
    cur.executemany(insert_sql, rows_to_insert)

    conn.commit()
    print("OK Committed successfully.")

except Exception as e:
    conn.rollback()
    print(f"\nERROR: {e}")
    sys.exit(1)
finally:
    cur.close()
    conn.close()

# ── Verify ────────────────────────────────────────────────────────────────────
print("\nVerification query:")
conn2 = psycopg2.connect(PG_CONN)
cur2  = conn2.cursor()
cur2.execute(f"SELECT COUNT(*) FROM {TARGET}")
total = cur2.fetchone()[0]
print(f"  Total rows in {TARGET}: {total}")

cur2.execute(f"""
    SELECT store_state, COUNT(*) AS stores
    FROM {TARGET}
    WHERE store_state IS NOT NULL AND TRIM(store_state) != ''
    GROUP BY store_state
    ORDER BY stores DESC
""")
print(f"\n  Stores by state:")
for state, cnt in cur2.fetchall():
    print(f"    {state:<20} {cnt}")

cur2.execute(f"""
    SELECT DISTINCT store_format FROM {TARGET}
    WHERE store_format IS NOT NULL AND TRIM(store_format) != ''
    ORDER BY store_format
""")
formats = [r[0] for r in cur2.fetchall()]
print(f"\n  Store formats: {formats}")

cur2.execute(f"""
    SELECT DISTINCT store_zone FROM {TARGET}
    WHERE store_zone IS NOT NULL AND TRIM(store_zone) != ''
    ORDER BY store_zone
""")
zones = [r[0] for r in cur2.fetchall()]
print(f"  Store zones:   {zones}")

cur2.close()
conn2.close()
print("\nDone.")
