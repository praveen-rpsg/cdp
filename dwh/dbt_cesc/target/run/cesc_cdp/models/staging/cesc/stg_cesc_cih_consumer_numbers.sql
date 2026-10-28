
  create view "cdp_meta"."staging"."stg_cesc_cih_consumer_numbers__dbt_tmp"
    
    
  as (
    

WITH source AS (
    SELECT
        cih_source_row_key,
        r1_source_ref,
        consumer_no_ss,
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
        match.ordinality AS consumer_number_ordinal,
        NULLIF(TRIM((match.match)[1]), '') AS consumer_no,
        s._loaded_at,
        s._source_file,
        s._source_system,
        s._ingestion_batch_id
    FROM source s
    CROSS JOIN LATERAL regexp_matches(
        s.consumer_no_ss,
        '''([^'']*)''',
        'g'
    ) WITH ORDINALITY AS match(match, ordinality)
)

SELECT *
FROM parsed
WHERE consumer_no IS NOT NULL
  );