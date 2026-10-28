select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    cesc_entity_id as unique_field,
    count(*) as n_records

from "cdp_meta"."silver_gold"."cesc_customer_360"
where cesc_entity_id is not null
group by cesc_entity_id
having count(*) > 1



      
    ) dbt_internal_test