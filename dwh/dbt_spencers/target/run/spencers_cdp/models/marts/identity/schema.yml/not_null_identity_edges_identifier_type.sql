
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select identifier_type
from "cdp_meta"."silver_identity"."identity_edges"
where identifier_type is null



  
  
      
    ) dbt_internal_test