
-- R1 ambiguity report (warn). Returns every r1_source_ref reached by more than one cesc_entity_id
-- (through the entities' primary CIH rows). This is NOT a reason to merge or drop entities: R1 is an auxiliary
-- source reference only. cesc_customer_360 keeps the entities separate and leaves their lifetime attributes NULL
-- (enforced as an error by tests/assert_cesc_customer_360_coverage.sql, branch ambiguous_r1_lifetime_populated).
select 'r1_maps_to_multiple_entities' as issue,
       c.r1_source_ref::text          as r1_source_ref,
       count(distinct n.cesc_entity_id::text) as entity_count
from "cdp_meta"."cesc_silver"."cesc_entity_nodes" n
inner join "cdp_meta"."staging"."stg_cesc_cih" c
    on c.cih_source_row_key = n.primary_cih_source_row_key
where c.r1_source_ref is not null
group by c.r1_source_ref::text
having count(distinct n.cesc_entity_id::text) > 1