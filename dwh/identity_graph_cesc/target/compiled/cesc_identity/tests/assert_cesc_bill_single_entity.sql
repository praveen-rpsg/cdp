-- Identity-integrity check (error). Fails (returns rows) for every RESOLVED bill observation that is linked to
-- MORE THAN ONE existing cesc_entity_id by an OBSERVED_ON_BILL edge.
-- Gold does not pick an entity for such a bill: cesc_customer_monthly excludes it. The fix belongs in identity
-- resolution, not in Gold. Set severity to 'warn' here if the team wants the build to continue while it is triaged.
with resolved as (
    select distinct bill_source_key::text as k
    from "cdp_meta"."cesc_silver"."cesc_identity_resolution"
    where resolution_status = 'RESOLVED'
)
select 'bill_linked_to_multiple_entities' as issue,
       e.to_node_id::text                 as bill_source_key,
       count(distinct en.cesc_entity_id::text) as entity_count
from "cdp_meta"."cesc_silver"."cesc_identity_edges" e
inner join resolved r  on r.k = e.to_node_id::text
inner join "cdp_meta"."cesc_silver"."cesc_entity_nodes" en on en.cesc_entity_id::text = e.from_node_id::text
where e.from_node_type = 'CESC_ENTITY'
  and e.to_node_type   = 'BILL_OBSERVATION'
  and e.edge_type      = 'OBSERVED_ON_BILL'
group by e.to_node_id::text
having count(distinct en.cesc_entity_id::text) > 1