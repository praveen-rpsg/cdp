
  
    

  create  table "cdp_meta"."silver"."int_identity_spine__dbt_tmp"
  
  
    as
  
  (
    

-- Identity spine: all identifier observations from all sources
-- Mobile is the universal join key; surrogate_id = hash(mobile)

WITH cih_identifiers AS (
    SELECT
        mobile,
        surrogate_id,
        name,
        email,
        pincode,
        store_code,
        source_system,
        NULL::DATE AS observed_at
    FROM "cdp_meta"."staging"."stg_cih_profiles"
    WHERE mobile IS NOT NULL
),

pos_identifiers AS (
    SELECT
        mobile,
        surrogate_id,
        NULL AS name,
        NULL AS email,
        NULL AS pincode,
        store_code,
        source_system,
        last_seen_at AS observed_at
    FROM "cdp_meta"."staging"."stg_bill_identifiers"
    WHERE mobile IS NOT NULL
),

nps_ids AS (
    SELECT
        CASE
            WHEN LENGTH(REGEXP_REPLACE(mobile, '[^0-9]', '', 'g')) = 10
            THEN REGEXP_REPLACE(mobile, '[^0-9]', '', 'g')
            ELSE mobile
        END AS mobile,
        NULL AS surrogate_id,
        NULL AS name, NULL AS email, NULL AS pincode,
        store_code,
        'NPS' AS source_system, bill_date::DATE AS observed_at
    FROM "cdp_meta"."bronze"."raw_nps_survey"
    WHERE mobile IS NOT NULL AND mobile != ''
),

yvm_ids AS (
    SELECT
        CASE
            WHEN LENGTH(REGEXP_REPLACE(mobile_no, '[^0-9]', '', 'g')) = 10
            THEN REGEXP_REPLACE(mobile_no, '[^0-9]', '', 'g')
            ELSE mobile_no
        END AS mobile,
        NULL AS surrogate_id,
        customer_name AS name,
        email_id AS email,
        pincode,
        store_code,
        'YVM' AS source_system,
        NULL::DATE AS observed_at
    FROM "cdp_meta"."bronze"."raw_yvm_feedback"
    WHERE mobile_no IS NOT NULL AND mobile_no != ''
),

cashback_ids AS (
    SELECT
        CASE
            WHEN LENGTH(REGEXP_REPLACE(mobile_number, '[^0-9]', '', 'g')) = 10
            THEN REGEXP_REPLACE(mobile_number, '[^0-9]', '', 'g')
            ELSE mobile_number
        END AS mobile,
        NULL AS surrogate_id,
        NULL AS name, NULL AS email, NULL AS pincode,
        NULL AS store_code,
        'PROMO' AS source_system, NULL::DATE AS observed_at
    FROM "cdp_meta"."bronze"."raw_promo_cashback"
    WHERE mobile_number IS NOT NULL AND mobile_number != ''
),

all_identifiers AS (
    SELECT * FROM cih_identifiers
    UNION ALL
    SELECT * FROM pos_identifiers
    UNION ALL
    SELECT * FROM nps_ids WHERE mobile IS NOT NULL
    UNION ALL
    SELECT * FROM yvm_ids WHERE mobile IS NOT NULL
    UNION ALL
    SELECT * FROM cashback_ids WHERE mobile IS NOT NULL
)

SELECT
    mobile,
    surrogate_id,
    name,
    email,
    pincode,
    store_code,
    source_system,
    observed_at,
    MD5(COALESCE(mobile,'') || '|' || COALESCE(email,'') || '|' || source_system) AS identifier_hash
FROM all_identifiers
  );
  