-- =============================================================================
-- 2. TRANSACTIONS (BILLS) — One row per bill/order (all brands)
-- =============================================================================
-- For Spencers/NBL: each row = one POS bill (offline) or one e-commerce order (online)
-- For FMCG: each row = one B2B/D2C order
-- For CESC: not used (billing in utility_billing table)
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
    bill_id                 STRING          COMMENT 'Unique bill/order ID',
    customer_id             STRING          COMMENT 'FK to customers',
    bill_date               DATE            COMMENT 'Date of transaction',
    bill_timestamp          TIMESTAMP       COMMENT 'Exact timestamp of transaction',

    -- Channel & Location
    channel                 STRING          COMMENT 'in_store, online, app, whatsapp, call_center',
    store_id                STRING          COMMENT 'Store ID for offline transactions',
    store_name              STRING,
    store_city              STRING,
    store_zone              STRING,
    store_format            STRING          COMMENT 'hypermarket, supermarket, express, gourmet',
    pos_terminal_id         STRING          COMMENT 'POS terminal for offline bills',
    checkout_type           STRING          COMMENT 'cashier, self_checkout, scan_and_go',

    -- Bill totals
    bill_total              DOUBLE          COMMENT 'Final bill amount after discounts',
    bill_subtotal           DOUBLE          COMMENT 'Pre-discount subtotal',
    discount_amount         DOUBLE          COMMENT 'Total discount on this bill',
    tax_amount              DOUBLE,
    bill_total_with_tax     DOUBLE,

    -- Line item summary
    line_item_count         INT             COMMENT 'Number of distinct SKUs in this bill',
    total_quantity          INT             COMMENT 'Total units across all line items',
    distinct_categories     INT             COMMENT 'Number of distinct categories in basket',
    distinct_departments    INT,

    -- Payment
    payment_method          STRING          COMMENT 'upi, credit_card, debit_card, cash, cod, wallet, net_banking',
    payment_status          STRING          COMMENT 'paid, pending, refunded, partial_refund',

    -- Loyalty
    loyalty_points_earned   INT,
    loyalty_points_redeemed INT,
    loyalty_card_used       BOOLEAN,

    -- Promotions
    coupon_code             STRING,
    coupon_discount         DOUBLE,
    promo_ids_applied       ARRAY<STRING>   COMMENT 'List of promotion IDs applied',

    -- Returns
    is_returned             BOOLEAN,
    return_amount           DOUBLE,
    return_date             DATE,

    -- Delivery (online orders)
    delivery_type           STRING          COMMENT 'home_delivery, click_and_collect, in_store',
    delivery_pincode        STRING,
    delivery_slot           STRING,

    -- Metadata
    _etl_loaded_at          TIMESTAMP
)
PARTITIONED BY (bill_month STRING COMMENT 'YYYY-MM partition key')
STORED AS PARQUET
LOCATION 's3://${BRAND}-datalake/gold/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- =============================================================================
-- 3. LINE_ITEMS — One row per item in a bill/basket (Spencers, NBL, FMCG)
-- =============================================================================
-- This is the bill line-item detail needed for basket analysis.
-- Joins to transactions on bill_id.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS line_items (
    line_item_id            STRING          COMMENT 'Unique line item ID',
    bill_id                 STRING          COMMENT 'FK to transactions.bill_id',
    customer_id             STRING          COMMENT 'Denormalized for direct customer-level queries',

    -- Product identification
    sku_id                  STRING          COMMENT 'Stock Keeping Unit ID',
    barcode                 STRING          COMMENT 'EAN/UPC barcode',
    product_name            STRING,
    brand_name              STRING          COMMENT 'Product brand (not store brand)',

    -- Taxonomy
    department              STRING          COMMENT 'Top-level: fresh_produce, packaged_food, beverages, home_care, personal_care, general_merchandise',
    category                STRING          COMMENT 'L2 category: dairy, snacks, cleaning, baby_care, pet_care, alcohol, etc.',
    sub_category            STRING          COMMENT 'L3 sub-category',
    segment                 STRING          COMMENT 'L4 segment (finest granularity)',

    -- Pricing
    mrp                     DOUBLE          COMMENT 'Maximum Retail Price',
    selling_price           DOUBLE          COMMENT 'Actual selling price per unit',
    line_total              DOUBLE          COMMENT 'selling_price * quantity',
    discount_per_unit       DOUBLE,
    line_discount           DOUBLE          COMMENT 'Total discount on this line',

    -- Quantity & Weight
    quantity                INT             COMMENT 'Number of units',
    unit_of_measure         STRING          COMMENT 'pcs, kg, g, l, ml, pack',
    weight_kg               DOUBLE          COMMENT 'Weight in kilograms (for weighted items)',
    pack_size               STRING          COMMENT 'Pack size descriptor: 500ml, 1kg, 12-pack',

    -- Product attributes
    is_private_label        BOOLEAN         COMMENT 'Is this a store-own brand?',
    is_organic              BOOLEAN,
    is_imported             BOOLEAN,
    is_premium              BOOLEAN         COMMENT 'Premium/gourmet tier product',
    is_promo                BOOLEAN         COMMENT 'Was this item on promotion?',
    promo_type              STRING          COMMENT 'bogo, pct_off, bundle, price_cut, combo',

    -- Dietary (food items)
    is_vegetarian           BOOLEAN,
    is_vegan                BOOLEAN,
    is_gluten_free          BOOLEAN,
    is_sugar_free           BOOLEAN,

    -- Returns
    is_returned             BOOLEAN,
    return_reason           STRING,

    -- Metadata
    bill_date               DATE            COMMENT 'Denormalized for partition pruning',
    _etl_loaded_at          TIMESTAMP
)
PARTITIONED BY (bill_month STRING COMMENT 'YYYY-MM partition key')
STORED AS PARQUET
LOCATION 's3://${BRAND}-datalake/gold/line_items/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
