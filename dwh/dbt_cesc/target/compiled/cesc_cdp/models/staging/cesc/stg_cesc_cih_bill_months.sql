

WITH source AS (
    SELECT
        cih_source_row_key,
        r1_source_ref,
        bill_ac_month_ss,
        _loaded_at,
        _source_file,
        _source_system,
        _ingestion_batch_id
    FROM "cdp_meta"."staging"."stg_cesc_cih"
),

parsed AS (
    SELECT
        s.cih_source_row_key,
        s.r1_source_ref,
        match.ordinality AS bill_month_ordinal,
        NULLIF(TRIM((match.match)[1]), '') AS bill_ac_month,
        s._loaded_at,
        s._source_file,
        s._source_system,
        s._ingestion_batch_id
    FROM source s
    CROSS JOIN LATERAL regexp_matches(
        s.bill_ac_month_ss,
        '''([^'']*)''',
        'g'
    ) WITH ORDINALITY AS match(match, ordinality)
)

SELECT *
FROM parsed
WHERE bill_ac_month IS NOT NULL