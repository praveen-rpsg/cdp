
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unified_id
from "cdp_meta"."silver_identity"."unified_profiles"
where unified_id is null



  
  
      
    ) dbt_internal_test