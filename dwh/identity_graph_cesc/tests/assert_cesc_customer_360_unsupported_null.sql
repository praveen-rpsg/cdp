-- Fails (returns a row) if any always-unsupported attribute is populated (guards against fabricated values).
-- digital_payment_status is included: its ONLINE/OFFLINE/NOPMNT mapping is not defined in the available spec.
-- Config-gated columns (lifetime_bill_value, annual_billed_value, persistent_arrear_flag, arrears_trend,
-- consumption_trend, billing_trend) are intentionally not listed here: they are NULL by default and only
-- populate when their approving var is set.
select count(*) as populated_rows
from {{ ref('cesc_customer_360') }}
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
   or most_active_channel is not null or most_active_time is not null or most_active_day is not null
having count(*) > 0
