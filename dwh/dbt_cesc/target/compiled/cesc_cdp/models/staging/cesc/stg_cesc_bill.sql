

WITH source AS (
    SELECT *
    FROM "cdp_meta"."bronze"."cesc_bill_monthly"
),

normalized AS (
    SELECT
        -- Identifiers
        NULLIF(TRIM(customer_id), '') AS customer_id,
        NULLIF(TRIM(bill_ac_month), '') AS bill_ac_month,
        NULLIF(TRIM(consumer_no), '') AS consumer_no,
        NULLIF(TRIM(consumer_check_digit), '') AS consumer_check_digit,

        -- Organization / address
        NULLIF(TRIM(regional_office), '') AS regional_office,
        NULLIF(TRIM(address_line_one), '') AS address_line_one,
        NULLIF(TRIM(address_line_two), '') AS address_line_two,
        NULLIF(TRIM(address_line_three), '') AS address_line_three,
        NULLIF(TRIM(address_line_four), '') AS address_line_four,
        NULLIF(TRIM(address_line_five), '') AS address_line_five,
        NULLIF(TRIM(address_line_six), '') AS address_line_six,
        NULLIF(TRIM(supply_address), '') AS supply_address,

        -- Financial values
        net_amount,
        rebate_amount,
        gross_amount,
        due_date,
        arrear_amount,
        adjustment_amount,
        govt_duty,
        meter_rent_amount,
        security_deposit_amount,
        energy_charges_amount,

        -- Consumption / load
        total_unit,
        load_kw,

        -- Consumer / supply attributes
        NULLIF(TRIM(consumer_type), '') AS consumer_type,
        NULLIF(TRIM(supply_type), '') AS supply_type,
        NULLIF(TRIM(road_code), '') AS road_code,
        NULLIF(TRIM(premises_no), '') AS premises_no,
        NULLIF(TRIM(lamp_post_no), '') AS lamp_post_no,
        NULLIF(TRIM(supply_mode), '') AS supply_mode,
        NULLIF(TRIM(tax_status), '') AS tax_status,
        NULLIF(TRIM(voltage_status), '') AS voltage_status,
        NULLIF(TRIM(consumer_status_code), '') AS consumer_status_code,
        avg_unit,
        NULLIF(TRIM(consumer_account_status), '') AS consumer_account_status,

        -- Bill event attributes
        NULLIF(TRIM(event_bill_ac_month), '') AS event_bill_ac_month,
        event_date,
        NULLIF(TRIM(event_type), '') AS event_type,
        NULLIF(TRIM(bill_type), '') AS bill_type,

        -- Charges / statuses
        fixed_demand_charges,
        NULLIF(TRIM(subsidy_status), '') AS subsidy_status,
        NULLIF(TRIM(advance_payment_status), '') AS advance_payment_status,
        advance_payment_adjustment_amount,
        monthly_variable_cost_adj_amount,
        NULLIF(TRIM(public_body_status_code), '') AS public_body_status_code,

        -- Bill / digital / contact / payment
        bill_date,
        NULLIF(TRIM(ebill_status), '') AS ebill_status,

         -- Bill email
        NULLIF(TRIM(email_id), '') AS email_raw,
        CASE
            WHEN TRIM(email_id) ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
            THEN LOWER(TRIM(email_id))
            ELSE NULL
        END AS email_normalized,
        CASE
            WHEN email_id IS NULL OR TRIM(email_id) = '' THEN NULL
            WHEN TRIM(email_id) ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN TRUE
            ELSE FALSE
        END AS email_valid_flag,

        -- Bill mobile
        NULLIF(TRIM(mobile_no), '') AS mobile_raw,
        CASE
            WHEN TRIM(mobile_no) ~ '^[0-9]{10}$'
            THEN TRIM(mobile_no)
            ELSE NULL
        END AS mobile_normalized,
        CASE
            WHEN mobile_no IS NULL OR TRIM(mobile_no) = '' THEN NULL
            WHEN TRIM(mobile_no) ~ '^[0-9]{10}$' THEN TRUE
            ELSE FALSE
        END AS mobile_valid_flag,

        NULLIF(TRIM(pin_cd), '') AS pin_cd,
        NULLIF(TRIM(mode_of_pmnt_processed), '') AS mode_of_pmnt_processed,
        NULLIF(TRIM(pmnt_mode_desc), '') AS pmnt_mode_desc,

        fppas_amount,

        -- Ingestion lineage
        _loaded_at,
        _source_file,
        _source_system,
        _ingestion_batch_id

    FROM source
)

SELECT *
FROM normalized