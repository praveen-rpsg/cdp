select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Gold grain checks. Fails (returns rows) if:
--   * (cesc_entity_id, bill_ac_month) is not unique in cesc_customer_monthly
--   * a source bill is the primary bill of more than one entity-month row
--   * cesc_entity_id is not unique in cesc_customer_360
select 'duplicate_entity_month' as issue,
       cesc_entity_id::text     as k1,
       bill_ac_month::text      as k2,
       count(*)                 as n
from "cdp_meta"."silver_gold"."cesc_customer_monthly"
group by cesc_entity_id, bill_ac_month
having count(*) > 1

union all

select 'primary_bill_in_multiple_rows',
       primary_bill_source_key,
       null,
       count(*)
from "cdp_meta"."silver_gold"."cesc_customer_monthly"
group by primary_bill_source_key
having count(*) > 1

union all

select 'duplicate_360_entity',
       cesc_entity_id::text,
       null,
       count(*)
from "cdp_meta"."silver_gold"."cesc_customer_360"
group by cesc_entity_id
having count(*) > 1
      
    ) dbt_internal_test