
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select bill_id
from "cdp_meta"."staging"."stg_bill_transactions"
where bill_id is null



  
  
      
    ) dbt_internal_test