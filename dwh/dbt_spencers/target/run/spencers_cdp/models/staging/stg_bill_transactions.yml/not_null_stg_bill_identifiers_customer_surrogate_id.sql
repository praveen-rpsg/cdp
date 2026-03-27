
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_surrogate_id
from "cdp_meta"."silver_staging"."stg_bill_identifiers"
where customer_surrogate_id is null



  
  
      
    ) dbt_internal_test