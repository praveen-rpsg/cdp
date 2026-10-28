select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select bill_ac_month
from "cdp_meta"."staging"."stg_cesc_cih_bill_months"
where bill_ac_month is null



      
    ) dbt_internal_test