
  
    

  create  table "cdp_meta"."cesc_silver"."cesc_entity_nodes__dbt_tmp"
  
  
    as
  
  (
    

WITH cih_keys AS (
    SELECT DISTINCT cih_source_row_key
    FROM "cdp_meta"."staging"."stg_cesc_cih"
    WHERE cih_source_row_key IS NOT NULL
),

cih_profiles AS (
    SELECT
        cih_source_row_key,
        name_normalized AS canonical_name,
        mobile_normalized AS canonical_mobile,
        email_normalized AS canonical_email,
        pincode AS canonical_pincode,
        _loaded_at AS first_seen_at
    FROM "cdp_meta"."staging"."stg_cesc_cih"
),

cih_entities AS (
    SELECT
        k.cih_source_row_key,
        EXISTS (
            SELECT 1
            FROM "cdp_meta"."cesc_silver"."cesc_identity_resolution" r
            WHERE r.resolved_cih_source_row_key = k.cih_source_row_key
              AND r.resolution_status = 'RESOLVED'
        ) AS has_resolved_bill
    FROM cih_keys k
),

cih_nodes AS (
    SELECT
        reg.cesc_entity_id,
        re.cih_source_row_key AS primary_cih_source_row_key,
        CASE
            WHEN re.has_resolved_bill THEN 'RESOLVED_CIH'
            ELSE 'CIH_PROFILE'
        END AS entity_type,
        p.canonical_name,
        p.canonical_mobile,
        p.canonical_email,
        p.canonical_pincode,
        p.first_seen_at,
        CLOCK_TIMESTAMP() AS last_updated_at,
        'CESC' AS source_system,
        'CESC_IR_202602_v1.0' AS run_id
    FROM cih_entities re
    JOIN cesc_silver.cesc_entity_id_registry reg
        ON re.cih_source_row_key = reg.cih_source_row_key
    JOIN cih_profiles p
        ON re.cih_source_row_key = p.cih_source_row_key
),

bill_only_nodes AS (
    SELECT
        m.cesc_entity_id,
        NULL::TEXT AS primary_cih_source_row_key,
        'BILL_ONLY' AS entity_type,
        NULL::TEXT AS canonical_name,
        CASE
            WHEN m.identity_key_type = 'PHONE'
            THEN m.identity_key_value
            ELSE NULL
        END AS canonical_mobile,
        NULL::TEXT AS canonical_email,
        NULL::TEXT AS canonical_pincode,
        NULL::TIMESTAMPTZ AS first_seen_at,
        CLOCK_TIMESTAMP() AS last_updated_at,
        'CESC' AS source_system,
        'CESC_IR_202602_v1.0' AS run_id
    FROM cesc_silver.cesc_bill_only_consumer_map m
)

SELECT * FROM cih_nodes

UNION ALL

SELECT * FROM bill_only_nodes
  );
  