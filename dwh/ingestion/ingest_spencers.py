#!/usr/bin/env python3
"""Spencer's CDP Data Warehouse - Raw Data Ingestion (psycopg v3)

Loads raw data files into PostgreSQL bronze schema tables from multiple sources:
  - RAW_DIR:    dated folders with BILL, Location, Promo, ZABM zips
  - SAMPLE_DIR: NPS, promo_cashback, festival_list, YVM, ecom, customer_data
  - CIH_DIR:    CIH parquet files (~20M rows)
"""

import os
import sys
import glob
import zipfile
import tempfile
import shutil
import logging
from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RAW_DIR = os.getenv("RAW_DIR", r"C:\RAW")
SAMPLE_DIR = os.getenv("SAMPLE_DIR", r"D:\WORK\Sample Files\01 Spencers\masked")
CIH_DIR = os.getenv("CIH_DIR", r"C:\CIH_parquet")
DB_URL = os.getenv("DATABASE_URL", "postgresql://cdp:cdp@localhost:5432/cdp_meta")


def get_conn():
    """Open a psycopg v3 connection."""
    return psycopg.connect(DB_URL, autocommit=False)


# ---------------------------------------------------------------------------
# DDL bootstrap
# ---------------------------------------------------------------------------
def run_ddl_files(conn):
    """Execute all DDL scripts in dwh/sql/ in sorted order."""
    sql_dir = Path(__file__).resolve().parent.parent / "sql"
    for sql_file in sorted(sql_dir.glob("*.sql")):
        logger.info(f"Executing DDL: {sql_file.name}")
        with open(sql_file, "r", encoding="utf-8") as f:
            sql = f.read()
        conn.execute(sql)
        conn.commit()


# ---------------------------------------------------------------------------
# Helper: bulk COPY via psycopg v3
# ---------------------------------------------------------------------------
def copy_df_to_table(conn, df, table_name):
    """Bulk-load a DataFrame into *table_name* using COPY FROM STDIN (CSV).

    All columns present in *df* are loaded.  String columns are scrubbed of
    embedded newlines/tabs and the sentinels 'nan'/'None' are mapped to NULL.
    """
    columns = list(df.columns)
    df_clean = df.copy()

    for c in columns:
        if df_clean[c].dtype == object:
            df_clean[c] = (
                df_clean[c]
                .astype(str)
                .str.replace(r"[\r\n\t]+", " ", regex=True)
            )
            df_clean[c] = df_clean[c].replace({"nan": None, "None": None, "": None})

    buf = StringIO()
    df_clean.to_csv(buf, index=False, header=False, sep=",", na_rep="")
    buf.seek(0)

    col_list = ", ".join(f'"{c}"' for c in columns)
    with conn.cursor() as cur:
        with cur.copy(
            f"COPY {table_name} ({col_list}) FROM STDIN (FORMAT csv, NULL '')"
        ) as copy:
            while data := buf.read(8192):
                copy.write(data.encode("utf-8"))
    conn.commit()


# ---------------------------------------------------------------------------
# Helper: iterate dated folders in RAW_DIR
# ---------------------------------------------------------------------------
def _dated_folders():
    """Yield (folder_name, folder_path) for each dated sub-directory in RAW_DIR."""
    if not os.path.isdir(RAW_DIR):
        logger.warning(f"RAW_DIR not found: {RAW_DIR}")
        return
    for entry in sorted(os.listdir(RAW_DIR)):
        full = os.path.join(RAW_DIR, entry)
        if os.path.isdir(full) and entry.isdigit():
            yield entry, full


# =========================================================================
# 1.  BILL DELTAS
# =========================================================================
# 76 columns — NO customer_surrogate_id
BILL_COLUMNS = [
    "bill_date", "plant", "plant_desc", "till_no", "bill_no", "line_item_no",
    "condition_type", "promo_indicator", "city", "city_desc", "store_format",
    "region", "region_desc", "manual_bill_date", "cal_year_month", "calendar_year",
    "calendar_year_week", "bill_start_time", "bill_end_time", "cashier_no",
    "cashier_name", "gstin", "deliv_slot", "deliv_challan_gen", "portal_order_no",
    "order_date_time", "manual_bill_no", "article", "article_desc", "segment",
    "segment_desc", "family", "family_desc", "class", "class_desc", "brick",
    "brick_desc", "manufacturer", "manufacturer_name", "brand", "brand_name",
    "base_unit", "base_unit_desc", "gift_item_indicator", "indicator_for_bill",
    "sales_return", "line_item_count", "liquidity_type", "mobile_number",
    "otp_feed_time", "otp_gen_time", "card_number_1", "card_number_2",
    "card_number_3", "card_number_4", "card_number_5", "card_number_6",
    "card_number_7", "card_number_8", "vka0_discount_value", "ka04_discount_value",
    "ka02_discount_value", "z006_discount_value", "z007_discount_value",
    "zpro_discount_value", "zfre_discount_value", "zcat_discount_value",
    "zemp_discount_value", "zbil_discount_value", "total_manual_bill_disc",
    "queue_length", "billed_mrp", "billed_qty", "total_mrp_value",
    "gross_sale_value", "total_discount",
]


def _read_bill_csvs_from_dir(directory):
    """Read all BILL_DELTA_*.csv files from *directory* and return a list of DataFrames."""
    pattern = os.path.join(directory, "BILL_DELTA_*.csv")
    csv_files = sorted(glob.glob(pattern))
    frames = []
    for fp in csv_files:
        try:
            df = pd.read_csv(fp, dtype=str)
            df.columns = [c.strip().lower() for c in df.columns]
            df["_source_file"] = os.path.basename(fp)
            # Keep only expected columns + _source_file
            for col in BILL_COLUMNS:
                if col not in df.columns:
                    df[col] = None
            frames.append(df[BILL_COLUMNS + ["_source_file"]])
        except Exception as e:
            logger.error(f"  Failed to read {fp}: {e}")
            continue
    return frames


def ingest_bill_deltas(conn):
    """Load BILL_DELTA CSVs from every dated folder in RAW_DIR.

    Each zip contains ~96 store-level CSVs.  The 20260129 folder may also
    contain an already-extracted BILL_ directory alongside the zip.
    """
    logger.info("=== Ingesting BILL DELTAS ===")
    total_rows = 0

    for date_name, date_path in _dated_folders():
        logger.info(f"  Processing date folder: {date_name}")

        # --- already-extracted BILL_ folder (e.g. 20260129) ---------------
        for entry in os.listdir(date_path):
            entry_path = os.path.join(date_path, entry)
            if os.path.isdir(entry_path) and entry.startswith("BILL"):
                frames = _read_bill_csvs_from_dir(entry_path)
                if frames:
                    batch_df = pd.concat(frames, ignore_index=True)
                    copy_df_to_table(conn, batch_df, "bronze.raw_bill_delta")
                    total_rows += len(batch_df)
                    logger.info(
                        f"    {date_name}/{entry}: {len(frames)} CSVs, "
                        f"{len(batch_df)} rows (extracted dir)"
                    )

        # --- zip files ----------------------------------------------------
        zip_pattern = os.path.join(date_path, "BILL_*.zip")
        for zf_path in sorted(glob.glob(zip_pattern)):
            tmp_dir = tempfile.mkdtemp(prefix="bill_")
            try:
                with zipfile.ZipFile(zf_path, "r") as zf:
                    zf.extractall(tmp_dir)

                frames = _read_bill_csvs_from_dir(tmp_dir)
                # Also search one level deeper (zip may contain a sub-folder)
                for sub in os.listdir(tmp_dir):
                    sub_path = os.path.join(tmp_dir, sub)
                    if os.path.isdir(sub_path):
                        frames.extend(_read_bill_csvs_from_dir(sub_path))

                if frames:
                    batch_df = pd.concat(frames, ignore_index=True)
                    copy_df_to_table(conn, batch_df, "bronze.raw_bill_delta")
                    total_rows += len(batch_df)
                    logger.info(
                        f"    {date_name}/{os.path.basename(zf_path)}: "
                        f"{len(frames)} CSVs, {len(batch_df)} rows"
                    )
            except Exception as e:
                logger.error(f"    Failed to process {zf_path}: {e}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"  -> Total BILL DELTA rows loaded: {total_rows}")


# =========================================================================
# 2.  LOCATION MASTER
# =========================================================================
LOCATION_RENAME = {
    "storecode": "store_code",
    "storename": "store_name",
    "storeformat": "store_format",
    "storezone": "store_zone",
    "storebusinessregion": "store_business_region",
    "storeregioncode": "store_region_code",
    "storestate": "store_state",
    "storecitycode": "store_city_code",
    "storecitydescription": "store_city_description",
    "storepincode": "store_pincode",
    "storeaddress": "store_address",
    "storeopeningdate": "store_opening_date",
    "storeclosingdate": "store_closing_date",
}

LOCATION_COLS = [
    "store_code", "store_name", "store_format", "store_zone",
    "store_business_region", "store_region_code", "store_state",
    "store_city_code", "store_city_description", "store_pincode",
    "store_address", "status", "store_opening_date", "store_closing_date",
]


def ingest_location_master(conn):
    """Load Location_Master CSVs (pipe-delimited) from every dated folder."""
    logger.info("=== Ingesting LOCATION MASTER ===")
    total_rows = 0

    for date_name, date_path in _dated_folders():
        zip_pattern = os.path.join(date_path, "Location_Master_*.csv.zip")
        for zf_path in sorted(glob.glob(zip_pattern)):
            tmp_dir = tempfile.mkdtemp(prefix="loc_")
            try:
                with zipfile.ZipFile(zf_path, "r") as zf:
                    zf.extractall(tmp_dir)

                for csv_file in glob.glob(os.path.join(tmp_dir, "*.csv")):
                    try:
                        df = pd.read_csv(csv_file, sep="|", dtype=str)
                        df.columns = [c.strip().lower() for c in df.columns]
                        df.rename(columns=LOCATION_RENAME, inplace=True)
                        for col in LOCATION_COLS:
                            if col not in df.columns:
                                df[col] = None
                        copy_df_to_table(
                            conn, df[LOCATION_COLS], "bronze.raw_location_master"
                        )
                        total_rows += len(df)
                    except Exception as e:
                        logger.error(f"    Failed to read {csv_file}: {e}")
                        continue

                logger.info(
                    f"  {date_name}/{os.path.basename(zf_path)}: loaded"
                )
            except Exception as e:
                logger.error(f"  Failed to process {zf_path}: {e}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"  -> Total LOCATION MASTER rows loaded: {total_rows}")


# =========================================================================
# 3.  PROMOTIONS (Promo_SRL)
# =========================================================================
def ingest_promotions(conn):
    """Load Promo_SRL CSVs (comma-delimited) from every dated folder."""
    logger.info("=== Ingesting PROMOTIONS ===")
    total_rows = 0

    for date_name, date_path in _dated_folders():
        zip_pattern = os.path.join(date_path, "Promo_SRL_*.csv.zip")
        for zf_path in sorted(glob.glob(zip_pattern)):
            tmp_dir = tempfile.mkdtemp(prefix="promo_")
            try:
                with zipfile.ZipFile(zf_path, "r") as zf:
                    zf.extractall(tmp_dir)

                for csv_file in glob.glob(os.path.join(tmp_dir, "*.csv")):
                    try:
                        df = pd.read_csv(csv_file, dtype=str)
                        df.columns = [c.strip().lower() for c in df.columns]
                        rename_map = {
                            "manufact": "manufacturer",
                            "offer_txt": "offer_text",
                        }
                        df.rename(columns=rename_map, inplace=True)
                        copy_df_to_table(conn, df, "bronze.raw_promotions")
                        total_rows += len(df)
                    except Exception as e:
                        logger.error(f"    Failed to read {csv_file}: {e}")
                        continue

                logger.info(
                    f"  {date_name}/{os.path.basename(zf_path)}: loaded"
                )
            except Exception as e:
                logger.error(f"  Failed to process {zf_path}: {e}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"  -> Total PROMOTION rows loaded: {total_rows}")


# =========================================================================
# 4.  ARTICLE MASTER (ZABM)
# =========================================================================
# Target column names — order matches the pipe-delimited ZABM CSVs.
# NOTE: "Unit of Dimension" appears THREE times; we handle duplicates below.
ZABM_TARGET_COLS = [
    "article", "article_description", "segment_code", "segment_desc",
    "family_code", "family_desc", "class_code", "class_desc",
    "brick_code", "brick_desc", "manufacturer_code", "manufacturer_desc",
    "brand_code", "brand_desc", "category_code", "category_desc",
    "subcategory_code", "subcategory_desc", "created_on", "last_change",
    "created_by", "clt", "article_type", "old_article_no", "base_uom",
    "lab_off", "valid_from", "ean_upc", "ct", "document", "pgr", "temp",
    "net_weight", "weight_uom", "status", "group_code", "disposal_status",
    "listing", "article_category", "length", "length_uom", "width",
    "width_uom", "height", "height_uom", "shelf_life",
    "remaining_shelf_life", "tagging", "season", "season_year",
    "hsn_code", "nbl_sku_code", "tgs",
]

# Mapping from original header names to target column names.
ZABM_RENAME = {
    "Article": "article",
    "Article Description": "article_description",
    "Segment Code": "segment_code",
    "Segment Desc": "segment_desc",
    "Family Code": "family_code",
    "Family Desc": "family_desc",
    "Class Code": "class_code",
    "Class Desc": "class_desc",
    "Brick Code": "brick_code",
    "Brick Desc": "brick_desc",
    "Manufacturer Code": "manufacturer_code",
    "Manufacture Desc": "manufacturer_desc",
    "Brand Code": "brand_code",
    "Brand Desc": "brand_desc",
    "Category Code": "category_code",
    "Category Desc": "category_desc",
    "Subcategory Code": "subcategory_code",
    "Subcategory Desc": "subcategory_desc",
    "Created On": "created_on",
    "Last Chng": "last_change",
    "Created by": "created_by",
    "Clt": "clt",
    "Article Type": "article_type",
    "Old article no.": "old_article_no",
    "BUn": "base_uom",
    "Lab Off.": "lab_off",
    "Valid From": "valid_from",
    "EAN/UPC": "ean_upc",
    "Ct": "ct",
    "Document": "document",
    "PGr": "pgr",
    "Temp": "temp",
    "Net Weight": "net_weight",
    "WUn": "weight_uom",
    "Status": "status",
    "GROUP": "group_code",
    "Disposal Status": "disposal_status",
    "Listing": "listing",
    "Article Category": "article_category",
    "Length": "length",
    "Width": "width",
    "Height": "height",
    "SLife": "shelf_life",
    "RShLi": "remaining_shelf_life",
    "Tagging": "tagging",
    "Seas": "season",
    "SeYr": "season_year",
    "HSNCODE": "hsn_code",
    "NBL SKU Code": "nbl_sku_code",
    "TGS": "tgs",
}


def _read_zabm_csv(filepath):
    """Read a single ZABM pipe-delimited CSV, handling duplicate column names."""
    df = pd.read_csv(filepath, sep="|", dtype=str, header=0)

    # Handle duplicate column names — pandas appends .1, .2 etc.
    # Mark true duplicates with a suffix so we can rename deterministically.
    cols = list(df.columns)
    new_cols = []
    seen = {}
    for c in cols:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_dup{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols

    # Apply the main rename map
    df.rename(columns=ZABM_RENAME, inplace=True)

    # "Unit of Dimension" appears 3 times → length_uom, width_uom, height_uom
    uod_mapping = {
        "Unit of Dimension": "length_uom",
        "Unit of Dimension_dup1": "width_uom",
        "Unit of Dimension_dup2": "height_uom",
    }
    df.rename(columns=uod_mapping, inplace=True)

    # Ensure all target columns exist
    for col in ZABM_TARGET_COLS:
        if col not in df.columns:
            df[col] = None

    return df[ZABM_TARGET_COLS]


def ingest_article_master(conn):
    """Load ZABM (article master) pipe-delimited CSVs from every dated folder."""
    logger.info("=== Ingesting ARTICLE MASTER (ZABM) ===")
    total_rows = 0

    for date_name, date_path in _dated_folders():
        zip_pattern = os.path.join(date_path, "ZABM_*.zip")
        for zf_path in sorted(glob.glob(zip_pattern)):
            tmp_dir = tempfile.mkdtemp(prefix="zabm_")
            try:
                with zipfile.ZipFile(zf_path, "r") as zf:
                    zf.extractall(tmp_dir)

                csv_files = glob.glob(os.path.join(tmp_dir, "**", "*.csv"), recursive=True)
                for csv_file in sorted(csv_files):
                    try:
                        df = _read_zabm_csv(csv_file)
                        copy_df_to_table(conn, df, "bronze.raw_article_master")
                        total_rows += len(df)
                    except Exception as e:
                        logger.error(f"    Failed to read {csv_file}: {e}")
                        continue

                logger.info(
                    f"  {date_name}/{os.path.basename(zf_path)}: loaded"
                )
            except Exception as e:
                logger.error(f"  Failed to process {zf_path}: {e}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"  -> Total ARTICLE MASTER rows loaded: {total_rows}")


# =========================================================================
# 5.  CIH PROFILES (parquet)
# =========================================================================
CIH_RENAME = {
    "BrandID_s": "brand_id",
    "R1_s": "r1",
    "Name_s": "name",
    "City_s": "city",
    "Created_At_dt": "created_at",
    "Customer_Group_s": "customer_group",
    "Device_History_s": "device_history",
    "DOB_dt": "dob",
    "dob_day_i": "dob_day",
    "dob_month_i": "dob_month",
    "dob_year_i": "dob_year",
    "Email_domain_s": "email_domain",
    "Last_Device_s": "last_device",
    "Occupation_s": "occupation",
    "PinCode_s": "pincode",
    "Status_s": "status",
    "StoreCode_s": "store_code",
    "Street_s": "street",
    "Region_s": "region",
    "Employee_s": "employee",
    "PD_preferred_Store_s": "pd_preferred_store",
    "PD_Subsciption_End_Date_dt": "pd_subscription_end_date",
    "WhatsApp_s": "whatsapp",
    "Age_i": "age",
    "DND_s": "dnd",
    "PD_Subscription_Renewal_Count_i": "pd_subscription_renewal_count",
    "Alternate_Street_1_s": "alternate_street_1",
    "Alternate_Street_2_s": "alternate_street_2",
    "Alternate_Street_3_s": "alternate_street_3",
    "Alternate_Street_4_s": "alternate_street_4",
    "Alternate_Street_5_s": "alternate_street_5",
    "Alternate_City_1_s": "alternate_city_1",
    "Alternate_City_2_s": "alternate_city_2",
    "Alternate_City_3_s": "alternate_city_3",
    "Alternate_City_4_s": "alternate_city_4",
    "Alternate_City_5_s": "alternate_city_5",
    "Alternate_Region_1_s": "alternate_region_1",
    "Alternate_Region_2_s": "alternate_region_2",
    "Alternate_Region_3_s": "alternate_region_3",
    "Alternate_Region_4_s": "alternate_region_4",
    "Alternate_Region_5_s": "alternate_region_5",
    "Alternate_PinCode_1_s": "alternate_pincode_1",
    "Alternate_PinCode_2_s": "alternate_pincode_2",
    "Alternate_PinCode_3_s": "alternate_pincode_3",
    "Alternate_PinCode_4_s": "alternate_pincode_4",
    "Alternate_PinCode_5_s": "alternate_pincode_5",
    "First_Name_s": "first_name",
    "Last_Name_s": "last_name",
    "PD_Store_Contact_1_s": "pd_store_contact_1",
    "PD_Store_Contact_2_s": "pd_store_contact_2",
    "GW_Customer_Flag_s": "gw_customer_flag",
    "Accepts_Email_Marketing_s": "accepts_email_marketing",
    "Accepts_SMS_Marketing_s": "accepts_sms_marketing",
    "Total_Orders_i": "total_orders",
    "Total_Spent_d": "total_spent",
    "Company_s": "company",
    "Country_s": "country",
    "EmailID_s": "email_id",
    "CUSTOMER_SURROGATE_ID": "customer_surrogate_id",
}

# Columns to drop (duplicates with .1 suffix)
CIH_DROP_COLS = [
    "PD_preferred_Store_s.1",
    "PD_Subsciption_End_Date_dt.1",
    "PD_Subscription_Renewal_Count_i.1",
]


def ingest_cih_profiles(conn):
    """Load all CIH parquet files into bronze.raw_cih_profiles."""
    logger.info("=== Ingesting CIH PROFILES (parquet) ===")

    if not os.path.isdir(CIH_DIR):
        logger.warning(f"CIH_DIR not found: {CIH_DIR}")
        return

    parquet_files = sorted(glob.glob(os.path.join(CIH_DIR, "*.parquet")))
    logger.info(f"  Found {len(parquet_files)} parquet files in {CIH_DIR}")

    total_rows = 0
    for idx, pf in enumerate(parquet_files, 1):
        try:
            df = pd.read_parquet(pf)

            # Drop known duplicate columns
            for drop_col in CIH_DROP_COLS:
                if drop_col in df.columns:
                    df.drop(columns=[drop_col], inplace=True)

            # Rename columns
            df.rename(columns=CIH_RENAME, inplace=True)

            # Convert everything to TEXT (str)
            for col in df.columns:
                df[col] = df[col].astype(str)

            # Replace sentinel values with None
            df.replace({"-1": None, "": None, "nan": None, "None": None, "NaT": None}, inplace=True)

            copy_df_to_table(conn, df, "bronze.raw_cih_profiles")
            total_rows += len(df)

            if idx % 5 == 0 or idx == len(parquet_files):
                logger.info(
                    f"  CIH progress: {idx}/{len(parquet_files)} files, "
                    f"{total_rows} rows so far"
                )
        except Exception as e:
            logger.error(f"  Failed to read {pf}: {e}")
            continue

    logger.info(f"  -> Total CIH PROFILE rows loaded: {total_rows}")


# =========================================================================
# 6.  Existing sources from SAMPLE_DIR
# =========================================================================
def ingest_customer_data(conn):
    """Load customer-data-27-09-2024.csv from SAMPLE_DIR."""
    fp = os.path.join(SAMPLE_DIR, "customer-data-27-09-2024.csv")
    logger.info(f"Ingesting customer data from {fp}")
    df = pd.read_csv(fp, dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    rename_map = {
        "createdat": "created_at",
        "lastupdatedby": "last_updated_by",
        "lastupdatedinstore": "last_updated_in_store",
        "lastupdatedon": "last_updated_on",
    }
    df.rename(columns=rename_map, inplace=True)
    table_cols = [
        "address", "created_at", "last_updated_by", "last_updated_in_store",
        "last_updated_on", "mobile", "name", "pincode",
    ]
    copy_df_to_table(conn, df[table_cols], "bronze.raw_customer_data")
    logger.info(f"  -> Loaded {len(df)} rows into bronze.raw_customer_data")


def ingest_nps_survey(conn):
    """Load NPS May_24.csv from SAMPLE_DIR."""
    fp = os.path.join(SAMPLE_DIR, "NPS May_24.csv")
    logger.info(f"Ingesting NPS survey from {fp}")
    df = pd.read_csv(fp, dtype=str, encoding="cp1252")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    rename_map = {
        "cleanliness_&_hygiene_of_store": "cleanliness_hygiene",
        "availability_of_products_you_wanted_to_buy_in_our_store": "product_availability",
        "quality_and_freshness_of_our_fruits/vegetables/fish/meat": "quality_freshness",
        "products_being_value_for_money": "value_for_money",
        "promotional_offers": "promotional_offers",
        "store_staff_assistance_in_your_shopping": "staff_assistance",
        "check-out_experience(smooth_and_fast)": "checkout_experience",
        "overall_rating_of_spencer's": "overall_rating",
    }
    for old, new in rename_map.items():
        for col in df.columns:
            if old in col or col == old:
                df.rename(columns={col: new}, inplace=True)
                break
    table_cols = [
        "mobile", "store_code", "bill_date", "cleanliness_hygiene",
        "product_availability", "quality_freshness", "value_for_money",
        "promotional_offers", "staff_assistance", "checkout_experience",
        "overall_rating",
    ]
    for c in table_cols:
        if c not in df.columns:
            df[c] = None
    copy_df_to_table(conn, df[table_cols], "bronze.raw_nps_survey")
    logger.info(f"  -> Loaded {len(df)} rows into bronze.raw_nps_survey")


def ingest_promo_cashback(conn):
    """Load SRL-PromoCashback20241001233002.csv from SAMPLE_DIR."""
    fp = os.path.join(SAMPLE_DIR, "SRL-PromoCashback20241001233002.csv")
    logger.info(f"Ingesting promo cashback from {fp}")
    df = pd.read_csv(fp, dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    table_cols = [
        "mobile_number", "promo_id", "start_date", "end_date",
        "create_date", "channel", "amount",
    ]
    copy_df_to_table(conn, df[table_cols], "bronze.raw_promo_cashback")
    logger.info(f"  -> Loaded {len(df)} rows into bronze.raw_promo_cashback")


def ingest_festival_list(conn):
    """Load FESTIVAL LIST_Updated.xlsx from SAMPLE_DIR."""
    fp = os.path.join(SAMPLE_DIR, "FESTIVAL LIST_Updated.xlsx")
    logger.info(f"Ingesting festival list from {fp}")
    df = pd.read_excel(fp, engine="openpyxl", dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["year"] = (
        pd.to_datetime(df["start_date"], errors="coerce")
        .dt.year.astype("Int64")
        .astype(str)
    )
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    table_cols = [
        "year", "campaign_name", "start_date", "end_date", "geography",
        "start_day_of_week", "end_day_of_week", "business_region", "zone", "brand",
    ]
    for c in table_cols:
        if c not in df.columns:
            df[c] = None
    copy_df_to_table(conn, df[table_cols], "bronze.raw_festival_list")
    logger.info(f"  -> Loaded {len(df)} rows into bronze.raw_festival_list")


def ingest_yvm_feedback(conn):
    """Load YVM June_24.xlsb from SAMPLE_DIR (subset of columns)."""
    fp = os.path.join(SAMPLE_DIR, "YVM June_24.xlsb")
    logger.info(f"Ingesting YVM feedback from {fp}")
    df = pd.read_excel(fp, engine="pyxlsb")

    selected = df.iloc[:, :28].copy()
    selected.columns = [
        "sl_no", "created_by", "store_city", "store_name", "store_code",
        "region", "mode_of_yvm", "reference_no", "cli_no", "pincode",
        "staff_behaviour", "store_cleanliness", "billing_efficiency",
        "product_quality", "products_availability", "overall_satisfaction",
        "feedback_text", "toll_free_notes", "salutation", "customer_name",
        "mobile_no", "landline_no", "email_id", "bill_date1", "bill_no1",
        "bill_amount1", "till_no1", "order_no1",
    ]

    if len(df.columns) > 74:
        selected["feedback_type"] = df.iloc[:, 74].values
        selected["feedback_class"] = df.iloc[:, 75].values
        selected["feedback_sub_class"] = df.iloc[:, 76].values
        selected["primary_tat"] = df.iloc[:, 77].values
        selected["categorized_by"] = df.iloc[:, 78].values
    else:
        for c in [
            "feedback_type", "feedback_class", "feedback_sub_class",
            "primary_tat", "categorized_by",
        ]:
            selected[c] = None

    if len(df.columns) > 178:
        selected["platform_type"] = df.iloc[:, 178].values
    else:
        selected["platform_type"] = None

    table_cols = [
        "sl_no", "created_by", "store_city", "store_name", "store_code",
        "region", "mode_of_yvm", "reference_no", "cli_no", "pincode",
        "staff_behaviour", "store_cleanliness", "billing_efficiency",
        "product_quality", "products_availability", "overall_satisfaction",
        "feedback_text", "toll_free_notes", "salutation", "customer_name",
        "mobile_no", "landline_no", "email_id", "bill_date1", "bill_no1",
        "bill_amount1", "till_no1", "order_no1",
        "feedback_type", "feedback_class", "feedback_sub_class",
        "primary_tat", "categorized_by", "platform_type",
    ]
    for c in table_cols:
        selected[c] = (
            selected[c]
            .astype(str)
            .replace({"nan": None, "None": None, "": None})
        )

    copy_df_to_table(conn, selected[table_cols], "bronze.raw_yvm_feedback")
    logger.info(f"  -> Loaded {len(selected)} rows into bronze.raw_yvm_feedback")


def ingest_ecom_category(conn):
    """Load ecom_category_20240307050330.csv from SAMPLE_DIR."""
    fp = os.path.join(SAMPLE_DIR, "ecom_category_20240307050330.csv")
    logger.info(f"Ingesting ecom categories from {fp}")
    df = pd.read_csv(fp, dtype=str)
    df.columns = ["id", "category_name"]
    copy_df_to_table(conn, df, "bronze.raw_ecom_category")
    logger.info(f"  -> Loaded {len(df)} rows into bronze.raw_ecom_category")


def ingest_ecom_product_master(conn):
    """Load ecom_productmaster_20240307050330.csv from SAMPLE_DIR."""
    fp = os.path.join(SAMPLE_DIR, "ecom_productmaster_20240307050330.csv")
    logger.info(f"Ingesting ecom product master from {fp}")
    df = pd.read_csv(fp, dtype=str)
    table_cols = [
        "sku", "name", "product_type", "visibility", "category", "sub_category",
        "sub_sub_category", "store_code", "quantity", "unit", "brand", "food_type",
        "capacity", "package_type", "config_type", "country_of_origin", "deep_url",
        "is_liquor", "status", "created_at", "updated", "style_code", "color",
        "size", "gender", "material", "occasion", "category2", "shape_fit",
        "weave_type", "festival_product", "sole", "lining", "pocket", "sleeve_type",
        "heel_type", "toe_shape", "neck_type", "fabric_type", "fabric",
        "sleeve_style", "heel_height", "dress_type", "is_active", "description",
        "level",
    ]
    df.columns = table_cols[: len(df.columns)]
    actual_cols = list(df.columns)
    copy_df_to_table(conn, df[actual_cols], "bronze.raw_ecom_product_master")
    logger.info(f"  -> Loaded {len(df)} rows into bronze.raw_ecom_product_master")


# =========================================================================
# MAIN
# =========================================================================
BRONZE_TABLES = [
    "bronze.raw_bill_delta",
    "bronze.raw_location_master",
    "bronze.raw_promotions",
    "bronze.raw_article_master",
    "bronze.raw_cih_profiles",
    "bronze.raw_customer_data",
    "bronze.raw_nps_survey",
    "bronze.raw_promo_cashback",
    "bronze.raw_festival_list",
    "bronze.raw_yvm_feedback",
    "bronze.raw_ecom_category",
    "bronze.raw_ecom_product_master",
]


def main():
    logger.info("Starting Spencer's DWH ingestion pipeline")
    conn = get_conn()
    try:
        # Step 1: Create schemas and tables
        run_ddl_files(conn)
        logger.info("DDL execution complete")

        # Step 2: Truncate all bronze tables
        logger.info("Truncating bronze tables ...")
        for tbl in BRONZE_TABLES:
            try:
                conn.execute(f"TRUNCATE TABLE {tbl}")
                conn.commit()
            except Exception:
                conn.rollback()  # table may not exist yet
        logger.info("Truncation complete")

        # Step 3: Ingest all sources
        ingest_bill_deltas(conn)
        ingest_location_master(conn)
        ingest_promotions(conn)
        ingest_article_master(conn)
        ingest_cih_profiles(conn)
        ingest_customer_data(conn)
        ingest_nps_survey(conn)
        ingest_promo_cashback(conn)
        ingest_festival_list(conn)
        ingest_yvm_feedback(conn)
        ingest_ecom_category(conn)
        ingest_ecom_product_master(conn)

        logger.info("All bronze layer ingestion complete!")

        # Step 4: Print summary
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schemaname || '.' || relname AS table_name,
                       n_live_tup AS row_count
                FROM pg_stat_user_tables
                WHERE schemaname = 'bronze'
                ORDER BY relname
            """)
            rows = cur.fetchall()
            logger.info("=== Bronze Layer Summary ===")
            for table_name, row_count in rows:
                logger.info(f"  {table_name}: {row_count:,} rows")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
