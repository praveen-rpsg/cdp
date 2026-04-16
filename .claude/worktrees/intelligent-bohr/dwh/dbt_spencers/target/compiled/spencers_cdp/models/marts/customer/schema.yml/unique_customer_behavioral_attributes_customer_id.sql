
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from "cdp_meta"."silver_reverse_etl"."customer_behavioral_attributes"
where customer_id is not null
group by customer_id
having count(*) > 1


