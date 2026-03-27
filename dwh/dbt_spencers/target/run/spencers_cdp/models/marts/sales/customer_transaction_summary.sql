
  
    

  create  table "cdp_meta"."silver_gold"."customer_transaction_summary__dbt_tmp"
  
  
    as
  
  (
    

WITH txn AS (
    SELECT * FROM "cdp_meta"."staging"."stg_bill_transactions"
    WHERE sales_return = FALSE
),

bills AS (
    SELECT * FROM "cdp_meta"."silver"."stg_bill_summary"
),

profiles AS (
    SELECT unified_id, canonical_mobile, surrogate_id
    FROM "cdp_meta"."silver_identity"."unified_profiles"
),

customer_base AS (
    SELECT
        t.mobile_number,
        MIN(t.bill_date) AS first_bill_date,
        MAX(t.bill_date) AS last_bill_date,
        (CURRENT_DATE - MAX(t.bill_date)) AS recency_days,
        (CURRENT_DATE - MIN(t.bill_date)) AS tenure_days,
        COUNT(DISTINCT t.bill_id) AS total_bills,
        COUNT(DISTINCT t.bill_date) AS total_visits,
        COALESCE(SUM(t.gross_sale_value), 0) AS total_spend,
        COALESCE(SUM(t.total_discount), 0) AS total_discount_amount,
        COUNT(DISTINCT DATE_TRUNC('month', t.bill_date)) AS distinct_months,
        COUNT(DISTINCT t.store_code) AS distinct_store_count,
        COUNT(DISTINCT t.article) AS distinct_article_count,
        COUNT(*) AS total_line_items
    FROM txn t
    WHERE t.mobile_number IS NOT NULL
    GROUP BY t.mobile_number
),

bill_flags AS (
    SELECT
        b.mobile_number,
        COUNT(CASE WHEN b.sales_return THEN 1 END) AS return_bill_count,
        COUNT(CASE WHEN b.promo_applied THEN 1 END) AS promo_bill_count,
        COUNT(CASE WHEN b.weekend_flag THEN 1 END) AS weekend_bill_count,
        COUNT(CASE WHEN b.wednesday_flag THEN 1 END) AS wednesday_bill_count,
        AVG(b.billing_time_secs) FILTER (WHERE b.billing_time_secs > 0 AND b.billing_time_secs < 7200) AS avg_billing_time_secs,
        (MAX(b.bill_date) - MIN(b.bill_date)) AS dgbt_fs
    FROM bills b
    WHERE b.mobile_number IS NOT NULL
    GROUP BY b.mobile_number
),

fav_store AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number,
        store_code AS fav_store_code,
        store_desc AS fav_store_name,
        store_format AS fav_store_type
    FROM (
        SELECT mobile_number, store_code, MAX(store_desc) AS store_desc, MAX(store_format) AS store_format,
               COUNT(DISTINCT bill_date) AS visit_count, SUM(gross_sale_value) AS spend
        FROM txn WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, store_code
    ) s
    ORDER BY mobile_number, visit_count DESC, spend DESC
),

fav_day AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number, TRIM(day_of_week) AS fav_day
    FROM (
        SELECT mobile_number, day_of_week, COUNT(DISTINCT bill_id) AS bill_count
        FROM txn WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, day_of_week
    ) d
    ORDER BY mobile_number, bill_count DESC
),

fav_article_spend AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number, article AS fav_article_by_spend, article_desc AS fav_article_by_spend_desc
    FROM (
        SELECT mobile_number, article, MAX(article_desc) AS article_desc, SUM(gross_sale_value) AS spend
        FROM txn WHERE mobile_number IS NOT NULL GROUP BY mobile_number, article
    ) a ORDER BY mobile_number, spend DESC
),

fav_article_nob AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number, article AS fav_article_by_nob, article_desc AS fav_article_by_nob_desc
    FROM (
        SELECT mobile_number, article, MAX(article_desc) AS article_desc, COUNT(DISTINCT bill_id) AS nob
        FROM txn WHERE mobile_number IS NOT NULL GROUP BY mobile_number, article
    ) a ORDER BY mobile_number, nob DESC
),

second_fav_spend AS (
    SELECT mobile_number, article AS second_fav_article_by_spend FROM (
        SELECT mobile_number, article, SUM(gross_sale_value) AS spend,
               ROW_NUMBER() OVER (PARTITION BY mobile_number ORDER BY SUM(gross_sale_value) DESC) AS rn
        FROM txn WHERE mobile_number IS NOT NULL GROUP BY mobile_number, article
    ) a WHERE rn = 2
),

second_fav_nob AS (
    SELECT mobile_number, article AS second_fav_article_by_nob FROM (
        SELECT mobile_number, article, COUNT(DISTINCT bill_id) AS nob,
               ROW_NUMBER() OVER (PARTITION BY mobile_number ORDER BY COUNT(DISTINCT bill_id) DESC) AS rn
        FROM txn WHERE mobile_number IS NOT NULL GROUP BY mobile_number, article
    ) a WHERE rn = 2
),

channel_pres AS (
    SELECT
        mobile_number,
        CASE
            WHEN COUNT(DISTINCT delivery_channel) > 1 THEN 'Omni'
            WHEN MAX(delivery_channel) = 'Online' THEN 'Online'
            ELSE 'Offline'
        END AS channel_presence
    FROM txn WHERE mobile_number IS NOT NULL
    GROUP BY mobile_number
),

assembled AS (
    SELECT
        cb.mobile_number,
        p.unified_id,
        p.surrogate_id,
        cb.first_bill_date,
        cb.last_bill_date,
        cb.recency_days,
        cb.tenure_days,
        cb.total_bills,
        cb.total_visits,
        cb.total_spend,
        CASE WHEN cb.total_bills > 0 THEN ROUND(cb.total_spend / cb.total_bills, 2) ELSE 0 END AS spend_per_bill,
        CASE WHEN cb.total_visits > 0 THEN ROUND(cb.total_spend / cb.total_visits, 2) ELSE 0 END AS spend_per_visit,
        CASE WHEN cb.total_bills > 0 THEN ROUND(cb.total_line_items::NUMERIC / cb.total_bills, 2) ELSE 0 END AS avg_items_per_bill,
        cb.total_discount_amount AS total_discount,
        cb.distinct_months,
        cb.distinct_store_count,
        cb.distinct_article_count,
        COALESCE(bf.dgbt_fs, 0) AS dgbt_fs,
        ROUND(COALESCE(bf.avg_billing_time_secs, 0)::NUMERIC, 2) AS avg_billing_time_secs,
        COALESCE(bf.return_bill_count, 0) AS return_bill_count,
        COALESCE(bf.promo_bill_count, 0) AS promo_bill_count,
        COALESCE(bf.weekend_bill_count, 0) AS weekend_bill_count,
        COALESCE(bf.wednesday_bill_count, 0) AS wednesday_bill_count,
        fs.fav_store_code, fs.fav_store_name, fs.fav_store_type,
        fd.fav_day,
        fas.fav_article_by_spend, fas.fav_article_by_spend_desc,
        fan.fav_article_by_nob, fan.fav_article_by_nob_desc,
        sfs.second_fav_article_by_spend,
        sfn.second_fav_article_by_nob,
        cp.channel_presence,
        NTILE(10) OVER (ORDER BY cb.total_spend) AS spend_decile,
        NTILE(10) OVER (ORDER BY cb.total_bills) AS nob_decile,
        NOW() AS updated_at
    FROM customer_base cb
    LEFT JOIN profiles p ON cb.mobile_number = p.canonical_mobile
    LEFT JOIN bill_flags bf ON cb.mobile_number = bf.mobile_number
    LEFT JOIN fav_store fs ON cb.mobile_number = fs.mobile_number
    LEFT JOIN fav_day fd ON cb.mobile_number = fd.mobile_number
    LEFT JOIN fav_article_spend fas ON cb.mobile_number = fas.mobile_number
    LEFT JOIN fav_article_nob fan ON cb.mobile_number = fan.mobile_number
    LEFT JOIN second_fav_spend sfs ON cb.mobile_number = sfs.mobile_number
    LEFT JOIN second_fav_nob sfn ON cb.mobile_number = sfn.mobile_number
    LEFT JOIN channel_pres cp ON cb.mobile_number = cp.mobile_number
),

-- L1 segment (Resulticks logic)
with_l1 AS (
    SELECT *,
        CASE
            WHEN fav_store_type IN ('Large', 'LARGE', 'Hyper') THEN
                CASE
                    WHEN spend_per_bill >= 1400 AND total_visits > 4 THEN 'HVHF'
                    WHEN spend_per_bill < 1400 AND total_visits > 4 THEN 'LVHF'
                    WHEN spend_per_bill >= 1400 AND total_visits <= 4 THEN 'HVLF'
                    ELSE 'LVLF'
                END
            ELSE
                CASE
                    WHEN spend_per_bill >= 750 AND total_visits > 5 THEN 'HVHF'
                    WHEN spend_per_bill < 750 AND total_visits > 5 THEN 'LVHF'
                    WHEN spend_per_bill >= 750 AND total_visits <= 5 THEN 'HVLF'
                    ELSE 'LVLF'
                END
        END AS l1_segment
    FROM assembled
),

-- L2 segment (Resulticks logic)
with_l2 AS (
    SELECT *,
        CASE
            WHEN fav_store_type IN ('Large', 'LARGE', 'Hyper') THEN
                CASE
                    WHEN recency_days <= 30 AND total_visits >= 4 AND spend_per_bill >= 1400 AND distinct_months >= 3 THEN 'STAR'
                    WHEN recency_days <= 30 AND total_visits >= 4 AND spend_per_bill < 1400 AND distinct_months >= 3 THEN 'LOYAL'
                    WHEN recency_days <= 30 AND total_bills = 1 THEN 'New'
                    WHEN recency_days <= 30 THEN 'Win Back'
                    WHEN recency_days <= 60 THEN 'ACTIVE'
                    WHEN recency_days <= 120 THEN 'Inactive'
                    WHEN recency_days <= 180 THEN 'LAPSER'
                    ELSE 'Deep Lapsed'
                END
            ELSE
                CASE
                    WHEN recency_days <= 30 AND total_visits >= 5 AND spend_per_bill >= 750 AND distinct_months >= 3 THEN 'STAR'
                    WHEN recency_days <= 30 AND total_visits >= 4 AND spend_per_bill < 750 AND distinct_months >= 3 THEN 'LOYAL'
                    WHEN recency_days <= 30 AND total_bills = 1 THEN 'New'
                    WHEN recency_days <= 30 THEN 'Win Back'
                    WHEN recency_days <= 60 THEN 'ACTIVE'
                    WHEN recency_days <= 120 THEN 'Inactive'
                    WHEN recency_days <= 180 THEN 'LAPSER'
                    ELSE 'Deep Lapsed'
                END
        END AS l2_segment
    FROM with_l1
)

SELECT * FROM with_l2
  );
  