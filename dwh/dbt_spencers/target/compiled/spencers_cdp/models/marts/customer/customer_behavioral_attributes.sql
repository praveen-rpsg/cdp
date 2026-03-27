

WITH profiles AS (
    SELECT * FROM "cdp_meta"."silver_identity"."unified_profiles"
),

txn_summary AS (
    SELECT * FROM "cdp_meta"."silver_gold"."customer_transaction_summary"
),

channel_summary AS (
    SELECT
        mobile_number,
        MAX(CASE WHEN delivery_channel = 'Store' THEN spend END) AS store_spend,
        MAX(CASE WHEN delivery_channel = 'Online' THEN spend END) AS online_spend,
        MAX(CASE WHEN delivery_channel = 'Store' THEN bills END) AS store_bills,
        MAX(CASE WHEN delivery_channel = 'Online' THEN bills END) AS online_bills
    FROM "cdp_meta"."silver_gold"."customer_channel_summary"
    GROUP BY mobile_number
)

SELECT
    p.unified_id AS customer_id,
    p.canonical_mobile AS mobile,
    p.display_name,
    p.email,
    p.city,
    p.pincode,
    p.region,
    p.registered_store,
    p.age,
    p.customer_group,
    p.occupation,
    p.whatsapp,
    p.dnd,
    p.gw_customer_flag,
    p.accepts_email_marketing,
    p.accepts_sms_marketing,
    p.surrogate_id,
    -- Transaction summary
    t.first_bill_date,
    t.last_bill_date,
    t.recency_days,
    t.tenure_days,
    t.total_bills,
    t.total_visits,
    t.total_spend,
    t.spend_per_bill,
    t.spend_per_visit,
    t.avg_items_per_bill,
    t.total_discount,
    t.distinct_months,
    t.distinct_store_count,
    t.distinct_article_count,
    t.dgbt_fs,
    t.avg_billing_time_secs,
    t.return_bill_count,
    t.promo_bill_count,
    t.weekend_bill_count,
    t.wednesday_bill_count,
    t.fav_store_code,
    t.fav_store_name,
    t.fav_store_type,
    t.fav_day,
    t.fav_article_by_spend,
    t.fav_article_by_spend_desc,
    t.fav_article_by_nob,
    t.fav_article_by_nob_desc,
    t.second_fav_article_by_spend,
    t.second_fav_article_by_nob,
    t.channel_presence,
    t.spend_decile,
    t.nob_decile,
    t.l1_segment,
    t.l2_segment,
    -- Channel breakdown
    cs.store_spend,
    cs.online_spend,
    cs.store_bills,
    cs.online_bills,
    -- Lifecycle stage
    CASE
        WHEN t.mobile_number IS NULL THEN 'Registered'
        WHEN t.recency_days <= 30 THEN 'Active'
        WHEN t.recency_days <= 90 THEN 'At Risk'
        WHEN t.recency_days <= 180 THEN 'Lapsed'
        ELSE 'Churned'
    END AS lifecycle_stage,
    -- RFM scores
    NTILE(5) OVER (ORDER BY COALESCE(t.recency_days, 9999) DESC) AS rfm_recency_score,
    NTILE(5) OVER (ORDER BY COALESCE(t.total_bills, 0)) AS rfm_frequency_score,
    NTILE(5) OVER (ORDER BY COALESCE(t.total_spend, 0)) AS rfm_monetary_score,
    NOW() AS computed_at
FROM profiles p
LEFT JOIN txn_summary t ON p.canonical_mobile = t.mobile_number
LEFT JOIN channel_summary cs ON p.canonical_mobile = cs.mobile_number