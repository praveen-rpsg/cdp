select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cih_source_row_key
from "cdp_meta"."staging"."stg_cesc_cih"
where cih_source_row_key is null



      
    ) dbt_internal_test