
  
    

  create  table "cdp_meta"."silver_gold"."cesc_customer_360__dbt_tmp"
  
  
    as
  
  (
    

















with nodes as (
    select
        n.cesc_entity_id::text as cesc_entity_id,
        n.primary_cih_source_row_key,
        n.entity_type,
        n.canonical_name,
        n.canonical_mobile,
        n.canonical_email,
        n.canonical_pincode,
        n.first_seen_at,
        n.last_updated_at
    from "cdp_meta"."cesc_silver"."cesc_entity_nodes" n
),

cih as (
    select * from (
        select
            c.cih_source_row_key,
            c.r1_source_ref,
            c.name_normalized,
            c.house_no,
            c.floor,
            c.street,
            c.city,
            c.pincode,
            c.email_normalized,
            c.mobile_normalized,
            c.dnd_raw,
            c.is_mobile_normalized,
            row_number() over (
                partition by c.cih_source_row_key
                order by c._loaded_at desc nulls last
            ) as rn
        FROM "cdp_meta"."staging"."stg_cesc_cih" c
        where c.cih_source_row_key in (select primary_cih_source_row_key from nodes)
    ) x
    where x.rn = 1
),

-- how many distinct canonical entities reach each R1 (through their primary CIH row). R1 is auxiliary only.
r1_map as (
    select
        c.r1_source_ref,
        count(distinct n.cesc_entity_id) as r1_entity_count
    from nodes n
    inner join cih c
        on c.cih_source_row_key = n.primary_cih_source_row_key
    where c.r1_source_ref is not null
    group by c.r1_source_ref
),

-- lifetime rows are eligible only for an R1 that maps to exactly one entity; ambiguous R1s stay unmatched (NULL)
lifetime as (
    select * from (
        select
            l.r1_source_ref,
            l.first_bill_date,
            l.last_bill_date,
            l.tenure,
            l.bill_count,
            l.lifetime_spend,
            l.recency_days,
            row_number() over (
                partition by l.r1_source_ref
                order by l._loaded_at desc nulls last, l.last_bill_date desc nulls last
            ) as rn
        FROM "cdp_meta"."staging"."stg_cesc_lifetime" l
        where l.r1_source_ref in (select r1_source_ref from r1_map where r1_entity_count = 1)
    ) x
    where x.rn = 1
),

ranked as (
    select
        m.*,
        row_number() over (
            partition by m.cesc_entity_id
            order by m.month_start desc nulls last, m.bill_ac_month::text desc
        ) as month_rank,
        extract(year from max(m.month_start) over (partition by m.cesc_entity_id))::int as annual_year_anchor
    from "cdp_meta"."silver_gold"."cesc_customer_monthly" m
),

latest as (
    select * from ranked where month_rank = 1
),

agg as (
    select
        cesc_entity_id,
        count(*) filter (where month_start is not null)                          as billed_months_available,
        min(month_start)                                                         as first_billed_month,
        max(month_start)                                                         as latest_billed_month,
        
        
        count(total_unit) filter (where month_start is not null and month_rank <= 3)                                 as consumption_months_3m,
        
        avg(total_unit) filter (where month_start is not null and month_rank <= 3)                                   as avg_consumption_3m,
        
        
        
        count(total_unit) filter (where month_start is not null and month_rank <= 6)                                 as consumption_months_6m,
        
        avg(total_unit) filter (where month_start is not null and month_rank <= 6)                                   as avg_consumption_6m,
        
        
        
        count(total_unit) filter (where month_start is not null and month_rank <= 12)                                 as consumption_months_12m,
        
        avg(total_unit) filter (where month_start is not null and month_rank <= 12)                                   as avg_consumption_12m,
        
        
        -- contract: stddev(consumption) / mean(consumption); period not specified -> 12-month window
        -- (longest window the contract defines), sample standard deviation; needs >= 2 months
        stddev_samp(total_unit) filter (where month_start is not null and month_rank <= 12)
            / nullif(avg(total_unit) filter (where month_start is not null and month_rank <= 12), 0)                as consumption_volatility,
        
        cast(null as double precision)                                           as consumption_slope,
        cast(null as double precision)                                           as arrear_slope,
        cast(null as double precision)                                           as billing_slope,
        
        
        cast(null as numeric)                                                    as lifetime_bill_value,
        cast(null as numeric)                                                    as annual_billed_value,
        
        max(annual_year_anchor)                                                  as annual_year,
        count(*) filter (
            where month_start is not null
              and extract(year from month_start)::int = annual_year_anchor)      as annual_months_available,
        sum(total_unit) filter (
            where month_start is not null
              and extract(year from month_start)::int = annual_year_anchor)      as annual_consumption
    from ranked
    group by cesc_entity_id
),

pm_counts as (
    select cesc_entity_id, payment_method, count(*) as c
    from ranked
    where month_start is not null and month_rank <= 12 and payment_method is not null
    group by cesc_entity_id, payment_method
),

pm_ranked as (
    select
        cesc_entity_id,
        payment_method,
        rank() over (partition by cesc_entity_id order by c desc) as rk
    from pm_counts
),

pm_pref as (
    select
        cesc_entity_id,
        case when count(*) filter (where rk = 1) > 1 then 'MIXED'
             else max(payment_method) filter (where rk = 1) end    as preferred_payment_method
    from pm_ranked
    group by cesc_entity_id
)

select
    -- ══ identity / lineage: cesc_entity_id is the ONLY UNIFY360 identity key ══
    n.cesc_entity_id,
    n.entity_type,
    n.primary_cih_source_row_key,
    c.r1_source_ref,                                                 -- source reference only
    rm.r1_entity_count,                                              -- >1 = ambiguous R1: lifetime attributes NULL
    n.first_seen_at                                                  as entity_first_seen_at,
    n.last_updated_at                                                as entity_last_updated_at,

    -- ══ (B) primary / latest source-record attributes (one real bill; customer_id is a source ref, not a key) ══
    lm.customer_id,
    lm.consumer_no,
    lm.check_digit,
    lm.consumer_type,
    lm.consumer_status_code,
    lm.consumer_account_status,
    lm.public_body_status_code                                       as public_body_status,
    lm.regional_office,
    lm.supply_address,
    lm.address_line_two,                                             -- ADDRESS_LINE_ONE is the customer NAME, never an address line
    lm.address_line_three,
    lm.address_line_four,
    lm.address_line_five,
    lm.address_line_six,
    lm.road_code,
    lm.premises_no,
    lm.lamp_post_no,
    lm.supply_type,
    lm.supply_mode,
    lm.load_kw,                                                      -- single-record LOAD of the primary bill (not summed)
    lm.voltage_status,
    lm.tax_status,
    lm.bill_ac_month,
    lm.bill_date,
    lm.due_date,
    lm.subsidy_status,
    lm.advance_payment_status,
    lm.bill_type,
    lm.ebill_status,
    lm.avg_unit                                                      as average_consumption,   -- single-record AVG_UNIT (never summed)
    lm.payment_channel,
    lm.payment_method,
    lm.digital_payment_status,                                       -- NULL: ONLINE/OFFLINE/NOPMNT mapping not defined in available spec; raw values in payment_channel
    lm.consumption_deviation_pct                                     as consumption_deviation, -- primary bill's own TOTAL_UNIT vs AVG_UNIT

    -- (B/C) contact fields: canonical entity value first, then CIH, then latest bill (validated) as fallback
    coalesce(n.canonical_email, c.email_normalized, lm.email_id)     as email_id,
    coalesce(n.canonical_mobile, c.mobile_normalized, lm.mobile_number) as mobile_number,
    -- (B/C) pincode: contract source is bill PIN_CD; canonical / CIH values only as fallback
    coalesce(lm.pin_cd::text, n.canonical_pincode::text, c.pincode::text) as pincode,

    -- ══ (A) entity-month aggregates for the latest billing month (sum over all resolved bills that month) ══
    lm.bills_in_month                                                as latest_month_bills_count,
    lm.consumer_numbers_in_month                                     as latest_month_consumer_numbers,
    lm.net_amount,
    lm.gross_amount,
    lm.rebate_amount,
    lm.arrear_amount,
    lm.adjustment_amount,
    lm.govt_duty,
    lm.meter_rent_amount,
    lm.security_deposit_amount,
    lm.energy_charges_amount,
    lm.fixed_demand_charges,
    lm.monthly_variable_cost_adjustment,
    lm.fppas_amount,
    lm.advance_payment_adjustment,
    lm.total_unit                                                    as monthly_consumption,

    -- ══ (C) canonical / CIH attributes ══
    coalesce(c.name_normalized, n.canonical_name)                    as customer_name,
    c.city,
    c.house_no                                                       as cih_house_no,      -- CIH address structure differs from the
    c.floor                                                          as cih_floor,         -- bill's; kept separate, not merged
    c.street                                                         as cih_street,
    c.dnd_raw                                                        as dnd,
    c.is_mobile_normalized                                           as mobile_capable,

    -- ══ (D) lifetime attributes ══
    lf.first_bill_date,
    lf.last_bill_date,
    lf.tenure                                                        as tenure_days,
    lf.tenure::numeric / 30.0                                        as tenure_months,     -- contract: TENURE / 30 (approx.)
    lf.bill_count,
    lf.lifetime_spend,
    lf.recency_days,

    -- ══ (E) derived from the latest month's aggregates (like-with-like inputs) ══
    lm.outstanding_burden_ratio,
    lm.arrear_persistence_months,
    lm.arrear_recovery_rate,
    lm.effective_bill_cost_per_unit,
    lm.energy_charge_contribution_pct                                as energy_charge_contribution,

    -- ══ (E) derived from history ══
    a.avg_consumption_3m,
    a.avg_consumption_6m,
    a.avg_consumption_12m,
    a.consumption_months_3m,
    a.consumption_months_6m,
    a.consumption_months_12m,
    a.consumption_volatility,
    pp.preferred_payment_method,                                     -- most frequent payment method, 12-month window; tie => MIXED
    case when a.arrear_slope is null then null
         when abs(a.arrear_slope) <= 0 then 'STABLE'
         when a.arrear_slope > 0 then 'INCREASING'
         else 'DECREASING' end                                       as arrears_trend,         -- NULL unless trend vars configured
    case when a.consumption_slope is null then null
         when abs(a.consumption_slope) <= 0 then 'STABLE'
         when a.consumption_slope > 0 then 'INCREASING'
         else 'DECREASING' end                                       as consumption_trend,     -- NULL unless trend vars configured
    case when a.billing_slope is null then null
         when abs(a.billing_slope) <= 0 then 'STABLE'
         when a.billing_slope > 0 then 'INCREASING'
         else 'DECREASING' end                                       as billing_trend,         -- NULL unless trend vars configured
    
    cast(null as boolean)                                            as persistent_arrear_flag,
    
    a.lifetime_bill_value,                                           -- NULL until cesc_bill_value_approved_basis is set
    a.annual_billed_value,                                           -- NULL until cesc_bill_value_approved_basis is set
    a.annual_consumption,
    a.annual_year,
    'WORKING_DEFAULT_CALENDAR_YEAR_OF_LATEST_BILL_MONTH' as annual_year_source,
    a.annual_months_available,

    -- ══ (F) unsupported / unapproved: always NULL ══
    cast(null as text)    as district,
    cast(null as text)    as state,
    cast(null as text)    as load_category,
    cast(null as text)    as tariff_type,
    cast(null as text)    as meter_number,
    cast(null as text)    as meter_type,
    cast(null as text)    as added_load,
    cast(null as text)    as theft_tag,
    cast(null as text)    as tenure_bucket,
    cast(null as numeric) as advance_payment_amount,
    cast(null as text)    as consumption_bucket,
    cast(null as text)    as consumption_slab,
    cast(null as text)    as seasonal_consumption_profile,
    cast(null as text)    as payment_method_stability,               -- no stable/variable rule or period in the contract
    cast(null as text)    as payment_channel_migration,
    cast(null as numeric) as monthly_payment_fulfilment_rate,
    cast(null as numeric) as on_time_payment_score,
    cast(null as numeric) as payment_consistency_score,
    cast(null as text)    as payment_behaviour_profile,
    cast(null as text)    as arrear_status,
    cast(null as text)    as consumption_anomaly_status,
    cast(null as text)    as bill_shock_status,
    cast(null as numeric) as annual_payment_fulfilment_rate,
    cast(null as text)    as app_status,
    cast(null as text)    as app_payment_status,
    cast(null as text)    as digital_engagement_status,
    cast(null as text)    as current_payment_status,
    cast(null as text)    as complaint_status,
    cast(null as text)    as complaint_behaviour,
    cast(null as text)    as service_behaviour,
    cast(null as text)    as digital_friction_status,
    cast(null as text)    as service_dependency,
    cast(null as text)    as customer_effort_status,
    cast(null as text)    as repeat_issue_status,
    cast(null as text)    as service_resolution_status,
    cast(null as text)    as digital_adoption_gap,
    cast(null as text)    as digital_self_service_status,
    cast(null as text)    as digital_migration_status,
    cast(null as text)    as device_type,
    cast(null as text)    as screen_resolution,
    cast(null as text)    as digital_language,
    cast(null as text)    as device_manufacturer,
    cast(null as date)    as app_installed_date,
    cast(null as text)    as app_version,
    cast(null as text)    as operating_system,
    cast(null as text)    as os_version,
    cast(null as text)    as browser,
    cast(null as text)    as most_active_channel,
    cast(null as text)    as most_active_time,
    cast(null as text)    as most_active_day,

    -- ══ history coverage / lineage ══
    a.billed_months_available,
    a.first_billed_month,
    a.latest_billed_month,
    current_timestamp                                                as gold_built_at
from nodes n
left join cih c
    on c.cih_source_row_key = n.primary_cih_source_row_key
left join r1_map rm
    on rm.r1_source_ref = c.r1_source_ref
left join lifetime lf
    on lf.r1_source_ref = c.r1_source_ref
left join agg a
    on a.cesc_entity_id = n.cesc_entity_id
left join latest lm
    on lm.cesc_entity_id = n.cesc_entity_id
left join pm_pref pp
    on pp.cesc_entity_id = n.cesc_entity_id
  );
  