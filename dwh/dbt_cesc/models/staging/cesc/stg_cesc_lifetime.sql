{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'cesc_lifetime_attributes') }}
),

normalized AS (
    SELECT
        -- Source reference
        NULLIF(TRIM(r1_s), '') AS r1_source_ref,

        -- Lifetime dates
        fbd_lifetime_dt AS first_bill_date,
        sbd_lifetime_dt AS second_bill_date,
        lbd_lifetime_dt AS last_bill_date,

        -- Lifetime metrics
        tenure_lifetime_i AS tenure,
        bills_lifetime_i AS bill_count,
        spend_lifetime_d AS lifetime_spend,
        spendperbill_lifetime_d AS spend_per_bill,
        recency_lifetime_i AS recency_days,
        totaldiscount_lifetime_d AS lifetime_discount,

        -- Ingestion lineage
        _loaded_at,
        _source_file,
        _source_system,
        _ingestion_batch_id

    FROM source
)

SELECT *
FROM normalized
