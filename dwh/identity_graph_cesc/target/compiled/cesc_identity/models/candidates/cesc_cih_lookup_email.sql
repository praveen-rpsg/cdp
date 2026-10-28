

WITH base AS (
    SELECT
        cih_source_row_key,
        email_normalized
    FROM "cdp_meta"."staging"."stg_cesc_cih"
    WHERE email_normalized IS NOT NULL
      AND email_valid_flag = TRUE
),
counts AS (
    SELECT
        email_normalized,
        COUNT(DISTINCT cih_source_row_key) AS email_cih_count
    FROM base
    GROUP BY email_normalized
)

SELECT
    b.cih_source_row_key,
    b.email_normalized,
    c.email_cih_count
FROM base b
JOIN counts c ON b.email_normalized = c.email_normalized