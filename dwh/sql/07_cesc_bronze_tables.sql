-- ============================================================================
-- CESC Bronze Layer: Raw landing tables for the CESC source
-- ============================================================================
-- Migration : 07_cesc_bronze_tables.sql
-- Author    : UNIFY360 CDP team
-- Created   : 2026-09-11
-- Purpose   : Adds four CESC-specific Bronze tables to the existing bronze
--             schema. This file is additive only; no existing objects are
--             modified or dropped.
--
-- Source    : Calcutta Electric Supply Corporation (CESC)
-- Ref sheets: cesc data masked.xlsx (cdp_bill_data_202406_03)
--             CIH_master (cih master sheet)
--             CESC lifetime attributes sheet
--
-- Conventions (matching 02_bronze_tables.sql):
--   * CREATE TABLE IF NOT EXISTS  -- idempotent, safe to re-run
--   * All source columns land as TEXT or an unambiguous scalar type.
--     No identity resolution, deduplication, or derived business attributes.
--   * Monetary / decimal measures (_d suffix) -> NUMERIC (exact; no float)
--   * Integer counts (_i suffix) -> INTEGER
--   * Date fields (_dt suffix) -> DATE
--   * _ss (multi-valued / searchable-set) fields -> TEXT, raw as delivered;
--     no splitting or flattening at the Bronze layer
--   * _b (boolean) fields -> TEXT; cast deferred to staging/silver
--   * Identifiers (consumer no., road code, premises no., etc.) -> TEXT
--   * Pipeline metadata (_loaded_at, _source_file, _source_system,
--     _ingestion_batch_id) appended after source columns; not source fields
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1.  bronze.cesc_cih_master
--     Raw CIH (Customer Information Hub) golden-record extract from CESC.
--     Source: CIH_master workbook, sheet "cih master", 29 source columns.
--
--     _ss columns are multi-valued in the source system (searchable-set).
--     They are preserved as raw TEXT strings exactly as delivered.
--     Do NOT split, flatten, or cast them at ingestion.
--
--     R1_s is a Resulticks source-system reference field.
--     It must NOT be treated as or promoted to a UNIFY360 identity key.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cesc_cih_master (

    -- Singleton profile fields (_s suffix) ----------------------------------

    -- Source: R1_s
    -- Resulticks source-system reference field.
    -- Not a primary key, identity key, or UNIFY360 key.
    r1_s                            TEXT,

    -- Source: Name_s
    name_s                          TEXT,

    -- Source: HouseNo_s
    house_no_s                      TEXT,

    -- Source: Floor_s
    floor_s                         TEXT,

    -- Source: Street_s
    street_s                        TEXT,

    -- Source: City_s
    city_s                          TEXT,

    -- Source: Pincode_s
    pincode_s                       TEXT,

    -- Source: EmailID_s
    email_id_s                      TEXT,

    -- Source: BrandID_s
    brand_id_s                      TEXT,

    -- Source: IsMobile_b  (_b = boolean flag in source system)
    -- Stored as TEXT; actual value encoding (Y/N, true/false, 1/0) to be
    -- confirmed and cast in staging.
    is_mobile_b                     TEXT,

    -- Source: MobileNo_s
    mobile_no_s                     TEXT,

    -- Source: Email_domain_s
    email_domain_s                  TEXT,

    -- Source: DND_s  (Do Not Disturb)
    dnd_s                           TEXT,

    -- Source: Mode_Of_Payment_s
    mode_of_payment_s               TEXT,

    -- Source: Payment_Mode_Description_s
    payment_mode_description_s      TEXT,

    -- Multi-valued / searchable-set fields (_ss suffix) ---------------------
    -- Landed as raw TEXT. Delimiter / encoding format determined at staging.

    -- Source: Bill_Ac_Month_ss
    bill_ac_month_ss                TEXT,

    -- Source: Consumer_No_ss
    consumer_no_ss                  TEXT,

    -- Source: Regional_Office_ss
    regional_office_ss              TEXT,

    -- Source: Consumer_Type_ss
    consumer_type_ss                TEXT,

    -- Source: Road_Code_ss
    road_code_ss                    TEXT,

    -- Source: Premises_No_ss
    premises_no_ss                  TEXT,

    -- Source: Lamp_Post_No_ss
    lamp_post_no_ss                 TEXT,

    -- Source: Event_Bill_Ac_Month_ss
    event_bill_ac_month_ss          TEXT,

    -- Source: Bill_Type_ss
    bill_type_ss                    TEXT,

    -- Source: Consumer_Account_status_ss  (mixed case preserved from source)
    consumer_account_status_ss      TEXT,

    -- Source: Ebill_Status_ss
    ebill_status_ss                 TEXT,

    -- Source: Supply_Mode_ss
    supply_mode_ss                  TEXT,

    -- Source: Supply_Type_ss
    supply_type_ss                  TEXT,

    -- Source: Conumer_status_Code_ss  [sic -- source misspelling retained in comment]
    consumer_status_code_ss         TEXT,

    -- Pipeline metadata (not source columns) --------------------------------
    _loaded_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file                    TEXT,
    _source_system                  TEXT        NOT NULL DEFAULT 'CESC',
    _ingestion_batch_id             TEXT
);

COMMENT ON TABLE bronze.cesc_cih_master IS
    'CESC CIH (Customer Information Hub) golden-record extract. '
    'Source: CIH_master workbook, sheet cih master (29 source columns). '
    '_ss columns are multi-valued searchable-set fields; do not split or flatten. '
    'r1_s is a Resulticks source-system reference field, not a UNIFY360 identity key.';

COMMENT ON COLUMN bronze.cesc_cih_master.r1_s IS
    'Source: R1_s. Resulticks source-system reference. Not a UNIFY360 identity key.';
COMMENT ON COLUMN bronze.cesc_cih_master.is_mobile_b IS
    'Source: IsMobile_b. Boolean flag in source system; landed as TEXT pending value-encoding confirmation.';
COMMENT ON COLUMN bronze.cesc_cih_master.consumer_status_code_ss IS
    'Source: Conumer_status_Code_ss -- note source misspelling "Conumer". Multi-valued; do not flatten.';


-- ----------------------------------------------------------------------------
-- 2.  bronze.cesc_lifetime_attributes
--     CESC consumer lifetime aggregate attributes.
--     Source: CESC lifetime attributes sheet, 10 source columns.
--
--     R1_s is a Resulticks source-system reference field.
--     It must NOT be treated as or promoted to a UNIFY360 identity key.
--
--     Type conventions follow source-column suffixes:
--       _dt  -> DATE
--       _i   -> INTEGER
--       _d   -> NUMERIC  (exact decimal; no floating-point)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cesc_lifetime_attributes (

    -- Source: R1_s
    -- Resulticks source-system reference field.
    -- Not a primary key, identity key, or UNIFY360 key.
    r1_s                            TEXT,

    -- Source: FBD_Lifetime_dt  (First Bill Date)
    fbd_lifetime_dt                 DATE,

    -- Source: SBD_Lifetime_dt  (Second Bill Date)
    sbd_lifetime_dt                 DATE,

    -- Source: LBD_Lifetime_dt  (Last Bill Date)
    lbd_lifetime_dt                 DATE,

    -- Source: TENURE_Lifetime_i  (tenure in days or months)
    tenure_lifetime_i               INTEGER,

    -- Source: BILLS_Lifetime_i  (total number of bills)
    bills_lifetime_i                INTEGER,

    -- Source: SPEND_Lifetime_d  (total lifetime spend)
    spend_lifetime_d                NUMERIC,

    -- Source: SPENDPERBILL_Lifetime_d  (average spend per bill)
    spendperbill_lifetime_d         NUMERIC,

    -- Source: RECENCY_Lifetime_i  (days since last bill)
    recency_lifetime_i              INTEGER,

    -- Source: TotalDiscount_Lifetime_d  (total lifetime discount)
    totaldiscount_lifetime_d        NUMERIC,

    -- Pipeline metadata (not source columns) --------------------------------
    _loaded_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file                    TEXT,
    _source_system                  TEXT        NOT NULL DEFAULT 'CESC',
    _ingestion_batch_id             TEXT
);

COMMENT ON TABLE bronze.cesc_lifetime_attributes IS
    'CESC consumer lifetime aggregate attributes. '
    'Source: CESC lifetime attributes sheet (10 source columns). '
    'r1_s is a Resulticks source-system reference field, not a UNIFY360 identity key. '
    'Monetary/decimal fields use NUMERIC; date fields use DATE; counts use INTEGER.';

COMMENT ON COLUMN bronze.cesc_lifetime_attributes.r1_s IS
    'Source: R1_s. Resulticks source-system reference. Not a UNIFY360 identity key.';
COMMENT ON COLUMN bronze.cesc_lifetime_attributes.fbd_lifetime_dt IS
    'Source: FBD_Lifetime_dt. First Bill Date for this consumer.';
COMMENT ON COLUMN bronze.cesc_lifetime_attributes.sbd_lifetime_dt IS
    'Source: SBD_Lifetime_dt. Second Bill Date for this consumer.';
COMMENT ON COLUMN bronze.cesc_lifetime_attributes.lbd_lifetime_dt IS
    'Source: LBD_Lifetime_dt. Last Bill Date for this consumer.';
COMMENT ON COLUMN bronze.cesc_lifetime_attributes.tenure_lifetime_i IS
    'Source: TENURE_Lifetime_i. Consumer tenure (days or months; unit to be confirmed).';
COMMENT ON COLUMN bronze.cesc_lifetime_attributes.recency_lifetime_i IS
    'Source: RECENCY_Lifetime_i. Days elapsed since the consumer last bill date.';


-- ----------------------------------------------------------------------------
-- 3.  bronze.cesc_bill_monthly
--     CESC monthly electricity bill data.
--     Source: cesc data masked.xlsx, sheet cdp_bill_data_202406_03.
--     53 source columns, in source-sheet order.
--
--     customer_id and consumer_no are source attributes only.
--     Neither is used as a UNIFY360 identity key.
--     Address lines are preserved even though address is excluded from
--     CESC identity-resolution logic.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cesc_bill_monthly (

    -- Col 01  CUSTOMER ID.
    -- Trailing period is CESC source convention. Source attribute only.
    customer_id                         TEXT,

    -- Col 02  BILL A/C MONTH
    -- Period string (format not guaranteed: may be YYYYMM or "Jun-2024").
    bill_ac_month                       TEXT,

    -- Col 03  CONSUMER NO.
    -- Source identifier. Not an identity key; not a person identity.
    consumer_no                         TEXT,

    -- Col 04  CHECK DIGIT (11TH DIGIT OF CONSUMER NO)
    -- Single-character check digit; TEXT avoids numeric truncation.
    consumer_check_digit                TEXT,

    -- Col 05  REGIONAL OFFICE
    regional_office                     TEXT,

    -- Col 06  ADDRESS_LINE ONE (NAME)  -- contains consumer name per source spec
    address_line_one                    TEXT,

    -- Col 07  ADDRESS_LINE TWO
    address_line_two                    TEXT,

    -- Col 08  ADDRESS_LINE THREE
    address_line_three                  TEXT,

    -- Col 09  ADDRESS_LINE FOUR
    address_line_four                   TEXT,

    -- Col 10  ADDRESS_LINE FIVE
    address_line_five                   TEXT,

    -- Col 11  ADDRESS_LINE SIX
    address_line_six                    TEXT,

    -- Col 12  SUPPLY ADDRESS
    supply_address                      TEXT,

    -- Col 13  NET AMOUNT (RS.)  -- exact decimal; no floating-point
    net_amount                          NUMERIC,

    -- Col 14  REBATE AMOUNT (RS.)
    rebate_amount                       NUMERIC,

    -- Col 15  GROSS AMOUNT (RS.)
    gross_amount                        NUMERIC,

    -- Col 16  DUE DATE
    due_date                            DATE,

    -- Col 17  ARREAR AMOUNT
    arrear_amount                       NUMERIC,

    -- Col 18  ADJUSTMENT AMOUNT
    adjustment_amount                   NUMERIC,

    -- Col 19  GOVT. DUTY (GD)
    govt_duty                           NUMERIC,

    -- Col 20  METER RENT AMOUNT
    meter_rent_amount                   NUMERIC,

    -- Col 21  SECURITY DEPOSITE AMOUNT  [sic -- source spelling preserved in comment]
    security_deposit_amount             NUMERIC,

    -- Col 22  ENERGY CHARGES AMOUNT
    energy_charges_amount               NUMERIC,

    -- Col 23  TOTAL UNIT
    total_unit                          NUMERIC,

    -- Col 24  LOAD (KW)  -- sanctioned load; a measure, not an identifier
    load_kw                             NUMERIC,

    -- Col 25  CONSUMER TYPE
    consumer_type                       TEXT,

    -- Col 26  SUPPLY TYPE
    supply_type                         TEXT,

    -- Col 27  ROAD CODE  -- infrastructure identifier; TEXT even if Excel renders numeric
    road_code                           TEXT,

    -- Col 28  PREMISES NO  -- infrastructure identifier
    premises_no                         TEXT,

    -- Col 29  LAMP POST NO  -- infrastructure identifier
    lamp_post_no                        TEXT,

    -- Col 30  SUPPLY MODE
    supply_mode                         TEXT,

    -- Col 31  TAX STATUS
    tax_status                          TEXT,

    -- Col 32  VOLTAGE STATUS
    voltage_status                      TEXT,

    -- Col 33  CONUMER STATUS CODE  [sic -- source column name misspelling retained in comment]
    consumer_status_code                TEXT,

    -- Col 34  AVG. UNIT
    avg_unit                            NUMERIC,

    -- Col 35  CONSUMER ACCOUNT STATUS
    consumer_account_status             TEXT,

    -- Col 36  EVENT BILL A/C MONTH  -- period string; TEXT for same reason as bill_ac_month
    event_bill_ac_month                 TEXT,

    -- Col 37  EVENT DATE
    event_date                          DATE,

    -- Col 38  EVENT TYPE
    event_type                          TEXT,

    -- Col 39  BILL TYPE
    bill_type                           TEXT,

    -- Col 40  FIXED DEMAND CHARGES
    fixed_demand_charges                NUMERIC,

    -- Col 41  SUBSIDY STATUS
    subsidy_status                      TEXT,

    -- Col 42  ADVANCE PAYMENT STATUS
    advance_payment_status              TEXT,

    -- Col 43  ADVANCE PAYMENT ADJUSTMENT AMOUNT
    advance_payment_adjustment_amount   NUMERIC,

    -- Col 44  MONTHLY VARIABLE COST ADJUSTMENT AMOUNT
    monthly_variable_cost_adj_amount    NUMERIC,

    -- Col 45  PUBLIC BODY STATUS CODE
    public_body_status_code             TEXT,

    -- Col 46  BILL DATE
    bill_date                           DATE,

    -- Col 47  EBILL STATUS
    ebill_status                        TEXT,

    -- Col 48  EMAIL ID
    email_id                            TEXT,

    -- Col 49  MOBILE NO
    mobile_no                           TEXT,

    -- Col 50  PIN_CD
    pin_cd                              TEXT,

    -- Col 51  MODE_OF_PMNT_PROCESSED
    mode_of_pmnt_processed              TEXT,

    -- Col 52  PMNT_MODE_DESC
    pmnt_mode_desc                      TEXT,

    -- Col 53  FPPAS AMOUNT
    -- FPPAS abbreviation not expanded in source; treated as monetary per context.
    fppas_amount                        NUMERIC,

    -- Pipeline metadata (not source columns) --------------------------------
    _loaded_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file                        TEXT,
    _source_system                      TEXT        NOT NULL DEFAULT 'CESC',
    _ingestion_batch_id                 TEXT
);

COMMENT ON TABLE bronze.cesc_bill_monthly IS
    'CESC monthly electricity bill data. One row per consumer per billing period. '
    'Source: cesc data masked.xlsx, sheet cdp_bill_data_202406_03 (53 source columns). '
    'Raw landing layer: no deduplication, identity resolution, or derived attributes. '
    'customer_id and consumer_no are source attributes only, not UNIFY360 identity keys.';

COMMENT ON COLUMN bronze.cesc_bill_monthly.customer_id IS
    'Source: CUSTOMER ID.  Source attribute only. Not a UNIFY360 identity key.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.consumer_no IS
    'Source: CONSUMER NO.  Source identifier. Not an identity key; not a person identity.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.consumer_check_digit IS
    'Source: CHECK DIGIT (11TH DIGIT OF CONSUMER NO). Single-character; stored as TEXT.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.bill_ac_month IS
    'Source: BILL A/C MONTH. Period string kept as TEXT to handle variable source formats.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.address_line_one IS
    'Source: ADDRESS_LINE ONE (NAME). Contains consumer name per CESC source spec.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.road_code IS
    'Source: ROAD CODE. Infrastructure identifier stored as TEXT.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.premises_no IS
    'Source: PREMISES NO. Infrastructure identifier stored as TEXT.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.lamp_post_no IS
    'Source: LAMP POST NO. Infrastructure identifier stored as TEXT.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.security_deposit_amount IS
    'Source: SECURITY DEPOSITE AMOUNT -- source column name contains typo "DEPOSITE".';
COMMENT ON COLUMN bronze.cesc_bill_monthly.consumer_status_code IS
    'Source: CONUMER STATUS CODE -- source column name contains typo "CONUMER".';
COMMENT ON COLUMN bronze.cesc_bill_monthly.event_bill_ac_month IS
    'Source: EVENT BILL A/C MONTH. Period string; kept TEXT for same reason as bill_ac_month.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.monthly_variable_cost_adj_amount IS
    'Source: MONTHLY VARIABLE COST ADJUSTMENT AMOUNT. Name abbreviated to adj for brevity.';
COMMENT ON COLUMN bronze.cesc_bill_monthly.fppas_amount IS
    'Source: FPPAS AMOUNT. Abbreviation not expanded in source; treated as monetary NUMERIC.';
COMMENT ON COLUMN bronze.cesc_bill_monthly._loaded_at IS
    'Pipeline metadata: UTC timestamp when this row was written to Bronze.';
COMMENT ON COLUMN bronze.cesc_bill_monthly._source_file IS
    'Pipeline metadata: basename of the source file (e.g. cesc_bill_202406.xlsx).';
COMMENT ON COLUMN bronze.cesc_bill_monthly._source_system IS
    'Pipeline metadata: always CESC for rows in this table.';
COMMENT ON COLUMN bronze.cesc_bill_monthly._ingestion_batch_id IS
    'Pipeline metadata: opaque batch identifier set by the ingestion script.';


-- ----------------------------------------------------------------------------
-- 4.  bronze.cesc_ingestion_log
--     Operational audit log for every CESC Bronze ingestion run.
--     One row per file per run. Carries no consumer or billing data.
--     Shares ingestion_batch_id with the three data tables above.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cesc_ingestion_log (

    -- Surrogate key (auto-generated; this table is a log, not a data table)
    log_id                  BIGSERIAL   PRIMARY KEY,

    -- Batch identifier shared with _ingestion_batch_id in the data tables
    ingestion_batch_id      TEXT        NOT NULL,

    -- Source system that produced the file
    source_system           TEXT        NOT NULL DEFAULT 'CESC',

    -- Logical dataset name within CESC: 'bill_monthly' | 'cih_master' | 'lifetime_attributes'
    dataset_name            TEXT        NOT NULL,

    -- Path or basename of the file that was processed
    source_file             TEXT,

    -- Run timing
    run_started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_finished_at         TIMESTAMPTZ,

    -- Outcome: 'SUCCESS' | 'PARTIAL' | 'FAILED'
    status                  TEXT,

    -- Row counts
    rows_read               BIGINT,
    rows_loaded             BIGINT,

    -- Error detail (NULL on success)
    error_message           TEXT,
    error_detail            TEXT,

    -- Free-form run-level metadata (file hash, environment, etc.)
    extra                   JSONB
);

COMMENT ON TABLE bronze.cesc_ingestion_log IS
    'Operational audit log for CESC Bronze ingestion runs. '
    'One row per file per run; no consumer or billing data stored here. '
    'status values: SUCCESS | PARTIAL | FAILED.';
