
    
    

with all_values as (

    select
        is_mobile_normalized as value_field,
        count(*) as n_records

    from "cdp_meta"."staging"."stg_cesc_cih"
    group by is_mobile_normalized

)

select *
from all_values
where value_field not in (
    True,False
)


