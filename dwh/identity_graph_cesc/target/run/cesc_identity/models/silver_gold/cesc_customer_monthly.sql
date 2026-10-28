
  
    

  create  table "cdp_meta"."silver_gold"."cesc_customer_monthly__dbt_tmp"
  
  
    as
  
  (
    





with eligible_bills as (
    -- Existing behavior: include bills resolved to CIH identities.
    select distinct
        r.bill_source_key::text as bill_source_key,
        'RESOLVED'::text as eligibility_type
    from "cdp_meta"."cesc_silver"."cesc_identity_resolution" r
    where r.resolution_status = 'RESOLVED'

    union

    -- New behavior: include only bills assigned to bill-only identities.
    select distinct
        r.bill_source_key::text as bill_source_key,
        'BILL_ONLY_IDENTITY'::text as eligibility_type
    from "cdp_meta"."cesc_silver"."cesc_identity_resolution" r
    where r.resolution_status = 'UNRESOLVED'
      and r.resolution_method = 'UNRESOLVED_NO_CIH_CANDIDATE'
),

-- Only accept the appropriate entity edge for each eligible bill.
bill_entity_edges as (
    select distinct
        e.to_node_id::text     as bill_source_key,
        n.cesc_entity_id::text as cesc_entity_id
    from "cdp_meta"."cesc_silver"."cesc_identity_edges" e
    inner join eligible_bills eb
        on eb.bill_source_key = e.to_node_id::text
    inner join "cdp_meta"."cesc_silver"."cesc_entity_nodes" n
        on n.cesc_entity_id::text = e.from_node_id::text
    where e.from_node_type = 'CESC_ENTITY'
      and e.to_node_type   = 'BILL_OBSERVATION'
      and e.edge_type      = 'OBSERVED_ON_BILL'
      and (
            eb.eligibility_type = 'RESOLVED'
            or (
                eb.eligibility_type = 'BILL_ONLY_IDENTITY'
                and e.resolution_method = 'BILL_ONLY_IDENTITY'
            )
      )
),
bill_entity as (
    select bill_source_key, min(cesc_entity_id) as cesc_entity_id
    from bill_entity_edges
    group by bill_source_key
    having count(*) = 1
),

bills as (
    select
        md5(concat_ws('|', b.consumer_no, b.bill_ac_month, b.customer_id)) as bill_source_key,
        b.customer_id,
        b.bill_ac_month,
        b.consumer_no,
        b.consumer_check_digit,
        b.regional_office,
        b.supply_address,
        b.address_line_two,
        b.address_line_three,
        b.address_line_four,
        b.address_line_five,
        b.address_line_six,
        b.pin_cd,
        b.consumer_type,
        b.supply_type,
        b.supply_mode,
        b.road_code,
        b.premises_no,
        b.lamp_post_no,
        b.voltage_status,
        b.tax_status,
        b.consumer_status_code,
        b.consumer_account_status,
        b.public_body_status_code,
        b.subsidy_status,
        b.advance_payment_status,
        b.bill_type,
        b.ebill_status,
        b.bill_date,
        b.due_date,
        nullif(upper(trim(b.mode_of_pmnt_processed::text)), '') as payment_channel,
        nullif(trim(b.pmnt_mode_desc::text), '')               as payment_method,
        case when lower(b.email_valid_flag::text) in ('true', 't', '1', 'y', 'yes')
             then b.email_normalized end                        as email_id,
        case when lower(b.mobile_valid_flag::text) in ('true', 't', '1', 'y', 'yes')
             then b.mobile_normalized end                       as mobile_number,
        b.net_amount::numeric                                   as net_amount,
        b.rebate_amount::numeric                                as rebate_amount,
        b.gross_amount::numeric                                 as gross_amount,
        b.arrear_amount::numeric                                as arrear_amount,
        b.adjustment_amount::numeric                            as adjustment_amount,
        b.govt_duty::numeric                                    as govt_duty,
        b.meter_rent_amount::numeric                            as meter_rent_amount,
        b.security_deposit_amount::numeric                      as security_deposit_amount,
        b.energy_charges_amount::numeric                        as energy_charges_amount,
        b.fixed_demand_charges::numeric                         as fixed_demand_charges,
        b.monthly_variable_cost_adj_amount::numeric             as monthly_variable_cost_adjustment,
        b.fppas_amount::numeric                                 as fppas_amount,
        b.advance_payment_adjustment_amount::numeric            as advance_payment_adjustment,
        b.total_unit::numeric                                   as total_unit,
        b.avg_unit::numeric                                     as avg_unit,
        b.load_kw::numeric                                      as load_kw,
        b._loaded_at
    from "cdp_meta"."staging"."stg_cesc_bill" b
),

bills_entity as (
    select
        be.cesc_entity_id,
        b.*,
        row_number() over (
            partition by b.bill_source_key
            order by b._loaded_at desc nulls last
        ) as src_dup_rn
    from bills b
    inner join bill_entity be
        on be.bill_source_key = b.bill_source_key
),

bills_ranked as (
    select
        x.*,
        row_number() over (
            partition by x.cesc_entity_id, x.bill_ac_month
            order by x.bill_date desc nulls last, x.net_amount desc nulls last, x.bill_source_key
        ) as primary_rn
    from bills_entity x
    where x.src_dup_rn = 1
),

monthly_agg as (
    select
        cesc_entity_id,
        bill_ac_month,
        count(*)                          as bills_in_month,
        count(distinct consumer_no)       as consumer_numbers_in_month,
        sum(net_amount)                   as net_amount,
        sum(rebate_amount)                as rebate_amount,
        sum(gross_amount)                 as gross_amount,
        sum(arrear_amount)                as arrear_amount,
        sum(adjustment_amount)            as adjustment_amount,
        sum(govt_duty)                    as govt_duty,
        sum(meter_rent_amount)            as meter_rent_amount,
        sum(security_deposit_amount)      as security_deposit_amount,
        sum(energy_charges_amount)        as energy_charges_amount,
        sum(fixed_demand_charges)         as fixed_demand_charges,
        sum(monthly_variable_cost_adjustment) as monthly_variable_cost_adjustment,
        sum(fppas_amount)                 as fppas_amount,
        sum(advance_payment_adjustment)   as advance_payment_adjustment,
        sum(total_unit)                   as total_unit,
        max(_loaded_at)                   as source_loaded_at
    from bills_ranked
    group by cesc_entity_id, bill_ac_month
),

primary_bill as (
    select * from bills_ranked where primary_rn = 1
),

base as (
    select
        a.cesc_entity_id,
        a.bill_ac_month,
        case when a.bill_ac_month::text ~ '^[0-9]{4}(0[1-9]|1[0-2])$'
             then to_date(a.bill_ac_month::text, 'YYYYMM') end          as month_start,
        a.bills_in_month,
        a.consumer_numbers_in_month,
        p.bill_source_key                                              as primary_bill_source_key,
        p.customer_id,
        p.consumer_no,
        p.consumer_check_digit                                         as check_digit,
        p.regional_office,
        p.supply_address,
        p.address_line_two,
        p.address_line_three,
        p.address_line_four,
        p.address_line_five,
        p.address_line_six,
        p.pin_cd,
        p.email_id,
        p.mobile_number,
        p.consumer_type,
        p.supply_type,
        p.supply_mode,
        p.road_code,
        p.premises_no,
        p.lamp_post_no,
        p.voltage_status,
        p.tax_status,
        p.consumer_status_code,
        p.consumer_account_status,
        p.public_body_status_code,
        p.subsidy_status,
        p.advance_payment_status,
        p.bill_type,
        p.ebill_status,
        p.bill_date,
        p.due_date,
        p.payment_channel,
        p.payment_method,
        cast(null as text)                                             as digital_payment_status,  -- mapping not defined in available spec
        a.net_amount,
        a.rebate_amount,
        a.gross_amount,
        a.arrear_amount,
        a.adjustment_amount,
        a.govt_duty,
        a.meter_rent_amount,
        a.security_deposit_amount,
        a.energy_charges_amount,
        a.fixed_demand_charges,
        a.monthly_variable_cost_adjustment,
        a.fppas_amount,
        a.advance_payment_adjustment,
        a.total_unit,
        p.total_unit                                                   as primary_bill_total_unit,
        p.avg_unit,
        p.load_kw,
        a.source_loaded_at
    from monthly_agg a
    inner join primary_bill p
        on  p.cesc_entity_id = a.cesc_entity_id
        and p.bill_ac_month  = a.bill_ac_month
),

seq as (
    select
        base.*,
        (extract(year from month_start) * 12 + extract(month from month_start))::int as month_seq,
        case when arrear_amount is null then null
             when arrear_amount > 0 then true
             else false end                                                          as has_arrear
    from base
),

islands as (
    select
        seq.*,
        lag(arrear_amount) over (partition by cesc_entity_id order by month_seq)     as previous_arrear_amount,
        -- gaps-and-islands: consecutive calendar months with arrear > 0
        case when has_arrear then
            month_seq - row_number() over (partition by cesc_entity_id, has_arrear order by month_seq)
        end                                                                          as island_key
    from seq
)

select
    cesc_entity_id,
    bill_ac_month,
    month_start,
    month_seq,
    bills_in_month,
    consumer_numbers_in_month,
    primary_bill_source_key,
    customer_id,
    consumer_no,
    check_digit,
    regional_office,
    supply_address,
    address_line_two,
    address_line_three,
    address_line_four,
    address_line_five,
    address_line_six,
    pin_cd,
    email_id,
    mobile_number,
    consumer_type,
    supply_type,
    supply_mode,
    road_code,
    premises_no,
    lamp_post_no,
    voltage_status,
    tax_status,
    consumer_status_code,
    consumer_account_status,
    public_body_status_code,
    subsidy_status,
    advance_payment_status,
    bill_type,
    ebill_status,
    bill_date,
    due_date,
    payment_channel,
    payment_method,
    digital_payment_status,
    net_amount,
    rebate_amount,
    gross_amount,
    arrear_amount,
    adjustment_amount,
    govt_duty,
    meter_rent_amount,
    security_deposit_amount,
    energy_charges_amount,
    fixed_demand_charges,
    monthly_variable_cost_adjustment,
    fppas_amount,
    advance_payment_adjustment,
    total_unit,
    primary_bill_total_unit,
    avg_unit,
    load_kw,

    -- derived (formulas exactly as in the CESC contract)
    (arrear_amount / nullif(net_amount, 0)) * 100                                    as outstanding_burden_ratio,
    ((primary_bill_total_unit - avg_unit) / nullif(avg_unit, 0)) * 100               as consumption_deviation_pct,
    (net_amount / nullif(total_unit, 0))                                            as effective_bill_cost_per_unit,
    (energy_charges_amount / nullif(gross_amount, 0)) * 100                          as energy_charge_contribution_pct,
    has_arrear                                                                       as has_arrear_flag,
    case when month_seq is null or has_arrear is null then null
         when has_arrear then row_number() over (partition by cesc_entity_id, island_key order by month_seq)
         else 0 end                                                                  as arrear_persistence_months,
    case when month_seq is not null and previous_arrear_amount > 0 and arrear_amount is not null
         then ((previous_arrear_amount - arrear_amount) / previous_arrear_amount) * 100
    end                                                                              as arrear_recovery_rate,
    case when month_seq is not null then previous_arrear_amount end                  as previous_arrear_amount,

    'net_amount'                                                                    as effective_cost_basis,
    source_loaded_at,
    current_timestamp                                                                as gold_built_at
from islands
  );
  