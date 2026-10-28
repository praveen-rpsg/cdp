select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select consumer_no
from "cdp_meta"."staging"."stg_cesc_cih_consumer_numbers"
where consumer_no is null



      
    ) dbt_internal_test