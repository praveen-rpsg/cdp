
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select bill_date
from "cdp_meta"."silver_gold"."daily_store_sales"
where bill_date is null



  
  
      
    ) dbt_internal_test