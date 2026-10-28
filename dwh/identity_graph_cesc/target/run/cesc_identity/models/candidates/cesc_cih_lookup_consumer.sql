
  
    

  create  table "cdp_meta"."cesc_silver"."cesc_cih_lookup_consumer__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT
        cih_source_row_key,
        consumer_no
    FROM "cdp_meta"."staging"."stg_cesc_cih_consumer_numbers"
    WHERE consumer_no IS NOT NULL
),
counts AS (
    SELECT
        consumer_no,
        COUNT(DISTINCT cih_source_row_key) AS consumer_count_for_no
    FROM base
    GROUP BY consumer_no
)

SELECT
    b.cih_source_row_key,
    b.consumer_no,
    c.consumer_count_for_no
FROM base b
JOIN counts c ON b.consumer_no = c.consumer_no
  );
  