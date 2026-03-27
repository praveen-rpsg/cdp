

-- Bill-level summary: one row per bill
-- Aggregates line items from stg_bill_transactions into bill-grain metrics

WITH txn AS (
    SELECT * FROM "cdp_meta"."staging"."stg_bill_transactions"
)

SELECT
    bill_date,
    store_code,
    store_format,
    MAX(store_desc)                                             AS store_desc,
    till_no,
    bill_no,
    bill_id,
    mobile_number,
    region,
    city,
    delivery_channel,

    -- Line / quantity metrics
    COUNT(*)                                                    AS line_item_count,
    SUM(gross_sale_value)                                       AS gross_sale_value,
    SUM(total_discount)                                         AS total_discount,
    SUM(total_mrp_value)                                        AS total_mrp_value,

    -- Billing timestamps
    MIN(bill_start_time)                                        AS bill_start_time,
    MAX(bill_end_time)                                          AS bill_end_time,
    CASE
        WHEN MIN(bill_start_time) ~ '^\d{2}:\d{2}:\d{2}$'
         AND MAX(bill_end_time)   ~ '^\d{2}:\d{2}:\d{2}$'
        THEN EXTRACT(EPOCH FROM (MAX(bill_end_time)::TIME - MIN(bill_start_time)::TIME))::INT
        ELSE NULL
    END                                                         AS billing_time_secs,

    -- Boolean roll-ups
    BOOL_OR(sales_return)                                       AS sales_return,
    BOOL_OR(promo_indicator = 'P')                              AS promo_applied,
    BOOL_OR(gift_item_indicator = 'Y')                          AS gift_indicator,
    BOOL_OR(COALESCE(NULLIF(TRIM(liquidity_type), ''), '') = 'LQ')
                                                                AS liquidity_indicator,

    -- Resulticks flags
    BOOL_OR(weekend_flag)                                       AS weekend_flag,
    BOOL_OR(wednesday_flag)                                     AS wednesday_flag,
    MAX(day_of_week)                                            AS day_of_week

FROM txn
GROUP BY
    bill_date,
    store_code,
    store_format,
    till_no,
    bill_no,
    bill_id,
    mobile_number,
    region,
    city,
    delivery_channel