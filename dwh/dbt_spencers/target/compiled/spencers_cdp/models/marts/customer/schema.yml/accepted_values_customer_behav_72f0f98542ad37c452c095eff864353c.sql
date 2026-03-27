
    
    

with all_values as (

    select
        lifecycle_stage as value_field,
        count(*) as n_records

    from "cdp_meta"."silver_reverse_etl"."customer_behavioral_attributes"
    group by lifecycle_stage

)

select *
from all_values
where value_field not in (
    'New','Active','Lapsed','Churned'
)


