select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT bill_source_key, COUNT(*)
FROM "cdp_meta"."cesc_silver"."cesc_bill_spine"
GROUP BY bill_source_key
HAVING COUNT(*) > 1
      
    ) dbt_internal_test