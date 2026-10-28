select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select consumer_number_ordinal
from "cdp_meta"."staging"."stg_cesc_cih_consumer_numbers"
where consumer_number_ordinal is null



      
    ) dbt_internal_test