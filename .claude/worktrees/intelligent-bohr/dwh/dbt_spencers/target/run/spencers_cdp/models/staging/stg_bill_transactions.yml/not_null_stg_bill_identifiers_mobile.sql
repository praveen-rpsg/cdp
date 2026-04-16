
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mobile
from "cdp_meta"."staging"."stg_bill_identifiers"
where mobile is null



  
  
      
    ) dbt_internal_test