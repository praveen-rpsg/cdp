
    
    

with all_values as (

    select
        identifier_type as value_field,
        count(*) as n_records

    from "cdp_meta"."silver_identity"."identity_edges"
    group by identifier_type

)

select *
from all_values
where value_field not in (
    'mobile','email','name','store_affinity'
)


