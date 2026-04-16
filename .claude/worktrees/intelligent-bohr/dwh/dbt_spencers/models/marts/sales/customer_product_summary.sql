{{ config(materialized='table', schema='silver_gold') }}

WITH txn AS (
    SELECT * FROM {{ ref('stg_bill_transactions') }}
    WHERE sales_return = FALSE AND mobile_number IS NOT NULL
)
SELECT
    mobile_number,
    brick, MAX(brick_desc) AS brick_desc,
    segment AS category_code, MAX(segment_desc) AS category_desc,
    family, MAX(family_desc) AS family_desc,
    COUNT(DISTINCT bill_id) AS bills,
    COUNT(DISTINCT bill_date) AS visits,
    SUM(gross_sale_value) AS spend,
    CASE WHEN COUNT(DISTINCT bill_id) > 0 THEN ROUND(SUM(gross_sale_value)/COUNT(DISTINCT bill_id), 2) ELSE 0 END AS spend_per_bill,
    SUM(total_discount) AS total_discount
FROM txn
GROUP BY mobile_number, brick, segment, family
