select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select bill_month_ordinal
from "cdp_meta"."staging"."stg_cesc_cih_bill_months"
where bill_month_ordinal is null



      
    ) dbt_internal_test