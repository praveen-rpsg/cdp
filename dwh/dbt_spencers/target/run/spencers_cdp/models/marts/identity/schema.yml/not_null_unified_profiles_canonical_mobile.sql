
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select canonical_mobile
from "cdp_meta"."silver_identity"."unified_profiles"
where canonical_mobile is null



  
  
      
    ) dbt_internal_test