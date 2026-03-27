
    
    

select
    customer_surrogate_id as unique_field,
    count(*) as n_records

from "cdp_meta"."silver_gold"."customer_transaction_summary"
where customer_surrogate_id is not null
group by customer_surrogate_id
having count(*) > 1


