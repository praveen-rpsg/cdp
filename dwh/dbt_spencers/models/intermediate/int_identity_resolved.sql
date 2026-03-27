{{ config(materialized='table', schema='silver') }}

-- Identity resolution: mobile number is the universal join key
-- CIH profiles are the master (20M); POS bill customers join via mobile
-- unified_id = MD5(mobile) for ALL profiles — same mobile = same person

WITH cih AS (
    SELECT * FROM {{ ref('stg_cih_profiles') }}
    WHERE mobile IS NOT NULL
),

-- Deduplicate CIH by mobile (keep first occurrence)
cih_deduped AS (
    SELECT DISTINCT ON (mobile) *
    FROM cih
    ORDER BY mobile, surrogate_id
),

-- POS bill customers: distinct real mobiles
bill_mobiles AS (
    SELECT DISTINCT mobile
    FROM {{ ref('stg_bill_identifiers') }}
    WHERE mobile IS NOT NULL
),

-- CIH profiles (LEFT JOIN to see which have POS transactions)
cih_profiles AS (
    SELECT
        MD5(c.mobile) AS unified_id,
        c.mobile AS canonical_mobile,
        c.surrogate_id,
        c.cih_original_surrogate_id,
        c.name,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.pincode,
        c.street,
        c.region,
        c.store_code,
        c.status,
        c.dob,
        c.age,
        c.customer_group,
        c.occupation,
        c.whatsapp,
        c.dnd,
        c.gw_customer_flag,
        c.accepts_email_marketing,
        c.accepts_sms_marketing,
        c.total_orders AS cih_total_orders,
        c.total_spent AS cih_total_spent,
        CASE WHEN b.mobile IS NOT NULL THEN 'CIH+POS' ELSE 'CIH' END AS primary_source,
        CASE WHEN b.mobile IS NOT NULL THEN TRUE ELSE FALSE END AS has_transactions
    FROM cih_deduped c
    LEFT JOIN bill_mobiles b ON c.mobile = b.mobile
),

-- POS-only customers (transacted but NOT in CIH)
pos_only AS (
    SELECT DISTINCT ON (bi.mobile)
        MD5(bi.mobile) AS unified_id,
        bi.mobile AS canonical_mobile,
        bi.surrogate_id,
        NULL AS cih_original_surrogate_id,
        NULL AS name, NULL AS first_name, NULL AS last_name,
        NULL AS email, NULL AS city, NULL AS pincode,
        NULL AS street, NULL AS region,
        bi.store_code,
        NULL AS status, NULL AS dob, NULL::INT AS age,
        NULL AS customer_group, NULL AS occupation,
        NULL AS whatsapp, NULL AS dnd, NULL AS gw_customer_flag,
        NULL AS accepts_email_marketing, NULL AS accepts_sms_marketing,
        NULL::INT AS cih_total_orders, NULL::NUMERIC AS cih_total_spent,
        'POS' AS primary_source,
        TRUE AS has_transactions
    FROM {{ ref('stg_bill_identifiers') }} bi
    LEFT JOIN cih_deduped c ON bi.mobile = c.mobile
    WHERE c.mobile IS NULL
    ORDER BY bi.mobile, bi.last_seen_at DESC
)

SELECT * FROM cih_profiles
UNION ALL
SELECT * FROM pos_only
