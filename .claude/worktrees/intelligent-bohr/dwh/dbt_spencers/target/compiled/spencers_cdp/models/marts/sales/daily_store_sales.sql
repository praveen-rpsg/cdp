-- ============================================================================
-- Gold Mart: Daily Store Sales Aggregation
-- ============================================================================
-- Aggregates bill-level transaction data into daily store-level sales metrics.
-- Feeds into gold.g_total_sales and gold.g_class_level_nob etc.

WITH txn AS (
    SELECT * FROM "cdp_meta"."staging"."stg_bill_transactions"
    WHERE sales_return = FALSE
),

-- Daily sales at store + article level
daily_article_sales AS (
    SELECT
        bill_date,
        store_code,
        article,
        article_desc,
        segment AS segment_code,
        segment_desc,
        family,
        family_desc,
        class,
        class_desc,
        brick,
        brick_desc,
        brand,
        brand_name,
        store_format,
        region AS region_code,
        region_desc,
        city,
        city_desc,
        'OFFLINE' AS sales_channel,
        SUM(billed_qty) AS quantity,
        SUM(gross_sale_value) AS gross_sales,
        SUM(total_discount) AS discount_amount,
        SUM(gross_sale_value - total_discount) AS net_sales,
        COUNT(*) AS line_item_count,
        COUNT(DISTINCT bill_id) AS num_bills
    FROM txn
    WHERE article IS NOT NULL
    GROUP BY
        bill_date, store_code, article, article_desc,
        segment, segment_desc, family, family_desc,
        class, class_desc, brick, brick_desc,
        brand, brand_name, store_format,
        region, region_desc, city, city_desc
)

SELECT
    bill_date,
    store_code,
    article AS article_code,
    article_desc AS article_description,
    segment_code,
    segment_desc,
    family AS family_code,
    family_desc,
    class AS class_code,
    class_desc,
    brick AS brick_code,
    brick_desc,
    brand AS brand_code,
    brand_name AS brand_desc,
    store_format,
    region_code,
    region_desc,
    city,
    city_desc,
    sales_channel,
    quantity,
    gross_sales,
    discount_amount,
    net_sales,
    line_item_count,
    num_bills,
    NOW() AS created_at
FROM daily_article_sales