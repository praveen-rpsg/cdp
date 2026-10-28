select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select bill_ac_month
from "cdp_meta"."silver_gold"."cesc_customer_monthly"
where bill_ac_month is null



      
    ) dbt_internal_test