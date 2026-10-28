select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        email_valid_flag as value_field,
        count(*) as n_records

    from "cdp_meta"."staging"."stg_cesc_cih"
    group by email_valid_flag

)

select *
from all_values
where value_field not in (
    True,False
)



      
    ) dbt_internal_test