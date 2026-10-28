select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT 1
WHERE (SELECT COUNT(*) FROM "cdp_meta"."staging"."stg_cesc_cih") <> 2191125
   OR (SELECT COUNT(*) FROM "cdp_meta"."staging"."stg_cesc_bill") <> 3730974
      
    ) dbt_internal_test