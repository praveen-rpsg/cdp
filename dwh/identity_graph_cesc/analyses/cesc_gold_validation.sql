-- CESC Gold validation for psql. Queries only the Gold tables in schema silver_gold.
-- Checks that need the identity tables (entity coverage, RESOLVED-bill reconciliation) are enforced in dbt with
-- ref(): tests/assert_cesc_customer_360_coverage.sql  (run: dbt test --select cesc_customer_360)
-- Bill -> entity integrity (a resolved bill linked to >1 cesc_entity_id) needs the identity edges and is enforced in
-- dbt: tests/assert_cesc_bill_single_entity.sql. Such bills are EXCLUDED from Gold, never assigned to one entity.
-- R1 ambiguity is enforced in dbt (tests/assert_cesc_r1_single_entity.sql) and mirrored below (14, 15) from Gold.

-- 1. 360 grain: expect 0 rows
select cesc_entity_id, count(*) as n
from silver_gold.cesc_customer_360
group by cesc_entity_id
having count(*) > 1;

-- 2. monthly grain: expect 0 rows
select cesc_entity_id, bill_ac_month, count(*) as n
from silver_gold.cesc_customer_monthly
group by cesc_entity_id, bill_ac_month
having count(*) > 1;

-- 3. each primary bill lands in exactly one entity-month row: expect 0 rows
select primary_bill_source_key, count(*) as n
from silver_gold.cesc_customer_monthly
group by primary_bill_source_key
having count(*) > 1;

-- 4. row counts
select (select count(*) from silver_gold.cesc_customer_360)                            as rows_360,
       (select count(*) from silver_gold.cesc_customer_360 where billed_months_available is null) as entities_without_bills,
       (select count(distinct cesc_entity_id) from silver_gold.cesc_customer_monthly)   as entities_with_bills,
       (select count(*) from silver_gold.cesc_customer_monthly)                         as rows_monthly,
       (select sum(bills_in_month) from silver_gold.cesc_customer_monthly)              as bills_combined_into_monthly;

-- 5. entity-months that combine more than one bill (where (A) sums differ from a single source record)
select bills_in_month, count(*) as entity_months
from silver_gold.cesc_customer_monthly
group by bills_in_month
order by bills_in_month;

-- 6. months of history per entity (drives how many 3M/6M/12M values are full windows)
select billed_months_available, count(*) as entities
from silver_gold.cesc_customer_360
group by billed_months_available
order by billed_months_available;

-- 7. full vs partial windows
select count(*) filter (where consumption_months_3m  = 3)  as full_3m,
       count(*) filter (where consumption_months_6m  = 6)  as full_6m,
       count(*) filter (where consumption_months_12m = 12) as full_12m,
       count(*) filter (where avg_consumption_12m is not null and consumption_months_12m < 12) as partial_12m
from silver_gold.cesc_customer_360;

-- 8. no hard-coded month: recompute each entity's 3M average independently from the monthly table
--    using its own latest 3 months; expect 0 rows
with ranked as (
    select cesc_entity_id, total_unit,
           row_number() over (partition by cesc_entity_id order by month_start desc) as rn
    from silver_gold.cesc_customer_monthly
    where month_start is not null
),
recomputed as (
    select cesc_entity_id, avg(total_unit) as avg_3m, count(total_unit) as n
    from ranked where rn <= 3 group by cesc_entity_id
)
select c.cesc_entity_id, c.avg_consumption_3m, r.avg_3m
from silver_gold.cesc_customer_360 c
join recomputed r using (cesc_entity_id)
where c.avg_consumption_3m is distinct from r.avg_3m
   or c.consumption_months_3m is distinct from r.n;

-- 9. no hard-coded month: distribution of each entity's latest billed month
select latest_billed_month, count(*) as entities
from silver_gold.cesc_customer_360
group by latest_billed_month
order by latest_billed_month desc
limit 12;

-- 10. AVG_UNIT and LOAD are single-record values: no monthly row may carry them from a summed source
--     (spot check: multi-bill months still hold one avg_unit / load_kw value; list a sample)
select cesc_entity_id, bill_ac_month, bills_in_month, total_unit, primary_bill_total_unit, avg_unit, load_kw
from silver_gold.cesc_customer_monthly
where bills_in_month > 1
limit 20;

-- 11. unsupported attributes must be entirely NULL: expect 0
select count(*) as populated_unsupported_rows
from silver_gold.cesc_customer_360
where digital_payment_status is not null or district is not null or state is not null or load_category is not null or tariff_type is not null
   or meter_number is not null or meter_type is not null or added_load is not null or theft_tag is not null
   or tenure_bucket is not null or advance_payment_amount is not null or consumption_bucket is not null
   or consumption_slab is not null or seasonal_consumption_profile is not null
   or payment_method_stability is not null or payment_channel_migration is not null
   or monthly_payment_fulfilment_rate is not null or on_time_payment_score is not null
   or payment_consistency_score is not null or payment_behaviour_profile is not null
   or arrear_status is not null or consumption_anomaly_status is not null or bill_shock_status is not null
   or annual_payment_fulfilment_rate is not null
   or app_status is not null or app_payment_status is not null or digital_engagement_status is not null
   or current_payment_status is not null or complaint_status is not null or complaint_behaviour is not null
   or service_behaviour is not null or digital_friction_status is not null or service_dependency is not null
   or customer_effort_status is not null or repeat_issue_status is not null or service_resolution_status is not null
   or digital_adoption_gap is not null or digital_self_service_status is not null or digital_migration_status is not null
   or device_type is not null or screen_resolution is not null or digital_language is not null
   or device_manufacturer is not null or app_installed_date is not null or app_version is not null
   or operating_system is not null or os_version is not null or browser is not null
   or most_active_channel is not null or most_active_time is not null or most_active_day is not null;

-- 12. config-gated columns: all NULL unless their var is set (approved basis, persistence N, trend window + tolerance)
select count(lifetime_bill_value)     as lifetime_bill_value_populated,
       count(annual_billed_value)     as annual_billed_value_populated,
       count(persistent_arrear_flag)  as persistent_arrear_flag_populated,
       count(arrears_trend)           as arrears_trend_populated,
       count(consumption_trend)       as consumption_trend_populated,
       count(billing_trend)           as billing_trend_populated
from silver_gold.cesc_customer_360;

-- 13. annual year provenance
select annual_year_source, annual_year, count(*) as entities
from silver_gold.cesc_customer_360
group by annual_year_source, annual_year
order by annual_year desc nulls last;

-- 14. R1 ambiguity (informational): source references shared by more than one cesc_entity_id.
--     These are NOT collapsed; the entities stay separate and their lifetime attributes are NULL.
select r1_source_ref, count(distinct cesc_entity_id) as entities
from silver_gold.cesc_customer_360
where r1_source_ref is not null
group by r1_source_ref
having count(distinct cesc_entity_id) > 1
order by entities desc, r1_source_ref
limit 50;

-- 15. ambiguous R1 must never carry lifetime attributes: expect 0
select count(*) as ambiguous_r1_with_lifetime
from silver_gold.cesc_customer_360
where r1_entity_count > 1
  and (first_bill_date is not null or last_bill_date is not null or tenure_days is not null
       or tenure_months is not null or bill_count is not null or lifetime_spend is not null
       or recency_days is not null);

-- 16. raw payment channel is preserved; digital_payment_status is NULL for every row (mapping not in spec)
select payment_channel, count(*) as entities, count(digital_payment_status) as digital_payment_status_populated
from silver_gold.cesc_customer_360
group by payment_channel
order by entities desc;
