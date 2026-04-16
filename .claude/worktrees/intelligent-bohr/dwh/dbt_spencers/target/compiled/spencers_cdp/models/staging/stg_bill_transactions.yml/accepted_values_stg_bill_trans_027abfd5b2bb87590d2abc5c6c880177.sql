
    
    

with all_values as (

    select
        delivery_channel as value_field,
        count(*) as n_records

    from "cdp_meta"."staging"."stg_bill_transactions"
    group by delivery_channel

)

select *
from all_values
where value_field not in (
    'Online','Store'
)


