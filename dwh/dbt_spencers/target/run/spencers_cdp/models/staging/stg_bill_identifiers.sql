
  create view "cdp_meta"."staging"."stg_bill_identifiers__dbt_tmp"
    
    
  as (
    

-- Extract distinct mobile identifiers from POS transactions
-- surrogate_id = hash of mobile, matching CIH convention for join
-- Mobile is the universal key across CIH and bill delta

SELECT DISTINCT
    mobile_number                                           AS mobile,
    'POS_' || MD5(mobile_number)                            AS surrogate_id,
    store_code,
    MAX(bill_date)                                          AS last_seen_at,
    'POS'                                                   AS source_system
FROM "cdp_meta"."staging"."stg_bill_transactions"
WHERE mobile_number IS NOT NULL
GROUP BY mobile_number, store_code
  );