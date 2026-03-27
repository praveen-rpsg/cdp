
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    unified_id as unique_field,
    count(*) as n_records

from "cdp_meta"."silver_identity"."unified_profiles"
where unified_id is not null
group by unified_id
having count(*) > 1



  
  
      
    ) dbt_internal_test