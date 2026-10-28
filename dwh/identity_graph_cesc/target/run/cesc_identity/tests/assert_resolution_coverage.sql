select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT b.bill_source_key
FROM "cdp_meta"."cesc_silver"."cesc_bill_spine" b
LEFT JOIN "cdp_meta"."cesc_silver"."cesc_identity_resolution" r
    ON b.bill_source_key = r.bill_source_key
WHERE r.bill_source_key IS NULL
      
    ) dbt_internal_test