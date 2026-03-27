
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mobile_number
from "cdp_meta"."silver_gold"."customer_channel_summary"
where mobile_number is null



  
  
      
    ) dbt_internal_test