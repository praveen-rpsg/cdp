
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    canonical_mobile as unique_field,
    count(*) as n_records

from "cdp_meta"."silver_identity"."unified_profiles"
where canonical_mobile is not null
group by canonical_mobile
having count(*) > 1



  
  
      
    ) dbt_internal_test