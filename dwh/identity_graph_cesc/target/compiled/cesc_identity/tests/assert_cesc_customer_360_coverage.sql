-- Fails (returns rows) on any of:
--   * 360 does not contain exactly the entities of cesc_entity_nodes (coverage is by cesc_entity_id only)
--   * monthly contains an entity that is not in 360
--   * a monthly primary bill is not RESOLVED in cesc_identity_resolution
--   * the number of bills combined into cesc_customer_monthly differs from the number of bills that are
--     RESOLVED, linked to EXACTLY ONE existing entity by an OBSERVED_ON_BILL edge, and present in stg_cesc_bill
--     (bills linked to several entities are excluded from Gold and reported by assert_cesc_bill_single_entity)
--   * an entity whose R1 is shared with another entity carries lifetime attributes (must be NULL)
with n as (select cesc_entity_id::text as id from "cdp_meta"."cesc_silver"."cesc_entity_nodes"),
g as (select cesc_entity_id::text as id from "cdp_meta"."silver_gold"."cesc_customer_360"),
m as (select distinct cesc_entity_id::text as id from "cdp_meta"."silver_gold"."cesc_customer_monthly"),

resolved as (
    select distinct bill_source_key::text as k
    from "cdp_meta"."cesc_silver"."cesc_identity_resolution"
    where resolution_status = 'RESOLVED'
),
linked as (
    select e.to_node_id::text as k
    from "cdp_meta"."cesc_silver"."cesc_identity_edges" e
    inner join resolved r on r.k = e.to_node_id::text
    inner join "cdp_meta"."cesc_silver"."cesc_entity_nodes" en on en.cesc_entity_id::text = e.from_node_id::text
    where e.from_node_type = 'CESC_ENTITY'
      and e.to_node_type   = 'BILL_OBSERVATION'
      and e.edge_type      = 'OBSERVED_ON_BILL'
    group by e.to_node_id::text
    having count(distinct en.cesc_entity_id::text) = 1
),
present as (
    select distinct md5(concat_ws('|', consumer_no, bill_ac_month, customer_id)) as k
    from "cdp_meta"."staging"."stg_cesc_bill"
),
expected as (select count(*) as c from linked l inner join present p on p.k = l.k),
actual   as (select coalesce(sum(bills_in_month), 0) as c from "cdp_meta"."silver_gold"."cesc_customer_monthly")

select 'in_nodes_not_in_360' as issue, id as detail from (select id from n except select id from g) a
union all
select 'in_360_not_in_nodes', id from (select id from g except select id from n) b
union all
select 'in_monthly_not_in_360', id from (select id from m except select id from g) c
union all
select 'monthly_primary_bill_not_resolved', primary_bill_source_key
from "cdp_meta"."silver_gold"."cesc_customer_monthly"
where primary_bill_source_key not in (select k from resolved)
union all
select 'resolved_bill_count_mismatch',
       'expected=' || e.c::text || ' actual=' || a.c::text
from expected e cross join actual a
where e.c <> a.c
union all
select 'ambiguous_r1_lifetime_populated', cesc_entity_id::text
from "cdp_meta"."silver_gold"."cesc_customer_360"
where r1_entity_count > 1
  and (first_bill_date is not null or last_bill_date is not null or tenure_days is not null
       or tenure_months is not null or bill_count is not null or lifetime_spend is not null
       or recency_days is not null)