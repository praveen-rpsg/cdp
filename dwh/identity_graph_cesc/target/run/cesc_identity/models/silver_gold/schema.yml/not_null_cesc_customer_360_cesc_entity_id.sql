select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cesc_entity_id
from "cdp_meta"."silver_gold"."cesc_customer_360"
where cesc_entity_id is null



      
    ) dbt_internal_test