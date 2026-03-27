

WITH txn AS (
    SELECT * FROM "cdp_meta"."staging"."stg_bill_transactions"
    WHERE sales_return = FALSE AND mobile_number IS NOT NULL
)
SELECT
    mobile_number,
    delivery_channel,
    COUNT(DISTINCT bill_id) AS bills,
    COUNT(DISTINCT bill_date) AS visits,
    SUM(gross_sale_value) AS spend,
    CASE WHEN COUNT(DISTINCT bill_id) > 0 THEN ROUND(SUM(gross_sale_value)/COUNT(DISTINCT bill_id), 2) ELSE 0 END AS spend_per_bill,
    CASE WHEN COUNT(DISTINCT bill_date) > 0 THEN ROUND(SUM(gross_sale_value)/COUNT(DISTINCT bill_date), 2) ELSE 0 END AS spend_per_visit,
    SUM(total_discount) AS total_discount,
    MIN(bill_date) AS first_bill_date,
    MAX(bill_date) AS last_bill_date
FROM txn
GROUP BY mobile_number, delivery_channel