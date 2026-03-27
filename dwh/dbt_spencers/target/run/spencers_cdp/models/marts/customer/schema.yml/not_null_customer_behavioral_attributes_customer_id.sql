
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from "cdp_meta"."silver_reverse_etl"."customer_behavioral_attributes"
where customer_id is null



  
  
      
    ) dbt_internal_test