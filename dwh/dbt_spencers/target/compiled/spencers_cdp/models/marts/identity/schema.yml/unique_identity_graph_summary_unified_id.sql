
    
    

select
    unified_id as unique_field,
    count(*) as n_records

from "cdp_meta"."silver_identity"."identity_graph_summary"
where unified_id is not null
group by unified_id
having count(*) > 1


