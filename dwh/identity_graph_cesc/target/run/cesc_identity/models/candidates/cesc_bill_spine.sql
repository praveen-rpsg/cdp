
  
    

  create  table "cdp_meta"."cesc_silver"."cesc_bill_spine__dbt_tmp"
  
  
    as
  
  (
    

WITH source AS (
    SELECT *
    FROM "cdp_meta"."staging"."stg_cesc_bill"
    WHERE bill_ac_month = '202602'
)

SELECT
    MD5(
        CONCAT_WS(
            '|',
            COALESCE(consumer_no, ''),
            COALESCE(bill_ac_month, ''),
            COALESCE(customer_id, '')
        )
    ) AS bill_source_key,
    customer_id,
    bill_ac_month,
    consumer_no,
    consumer_check_digit,
    regional_office,
    NULLIF(TRIM(address_line_one), '') AS bill_name_raw,
    supply_address,
    pin_cd,
    mobile_raw,
    mobile_normalized,
    mobile_valid_flag,
    email_raw,
    email_normalized,
    email_valid_flag,
    net_amount,
    gross_amount,
    due_date,
    bill_date,
    ebill_status,
    _loaded_at,
    _source_file,
    _ingestion_batch_id
FROM source
  );
  