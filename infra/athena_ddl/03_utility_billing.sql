-- =============================================================================
-- 4. UTILITY_BILLING — One row per billing cycle (Power CESC only)
-- =============================================================================
-- Captures each monthly billing cycle with consumption, payment, and dues.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS utility_billing (
    billing_id              STRING          COMMENT 'Unique billing record ID',
    customer_id             STRING          COMMENT 'FK to customers',
    account_number          STRING          COMMENT 'Utility connection account number',

    -- Billing cycle
    bill_date               DATE            COMMENT 'Bill generation date',
    billing_period_start    DATE,
    billing_period_end      DATE,
    due_date                DATE            COMMENT 'Payment due date',
    is_current              BOOLEAN         COMMENT 'Is this the current/latest bill?',

    -- Consumption
    consumption_kwh         DOUBLE          COMMENT 'Consumption in kWh for this cycle',
    consumption_units       INT             COMMENT 'Meter units consumed',
    previous_reading        DOUBLE,
    current_reading         DOUBLE,
    reading_type            STRING          COMMENT 'actual, estimated, smart_meter',
    consumption_slab        STRING          COMMENT '0-100_units, 101-300_units, etc.',
    sanctioned_load_kw      DOUBLE,
    load_factor             DOUBLE,

    -- Bill amounts
    bill_amount             DOUBLE          COMMENT 'Total bill amount',
    energy_charges          DOUBLE,
    demand_charges          DOUBLE,
    fuel_adjustment         DOUBLE,
    fixed_charges           DOUBLE,
    tax_amount              DOUBLE,
    late_payment_surcharge  DOUBLE,
    arrears                 DOUBLE,
    subsidy_amount          DOUBLE,
    net_payable             DOUBLE          COMMENT 'Final amount to pay',

    -- Payment
    payment_date            DATE            COMMENT 'When payment was received',
    payment_amount          DOUBLE          COMMENT 'Amount paid',
    payment_mode            STRING          COMMENT 'online_portal, upi, net_banking, credit_card, debit_card, cash_counter, cheque, auto_debit, wallet',
    payment_status          STRING          COMMENT 'paid, partial, unpaid, overdue',
    outstanding_amount      DOUBLE          COMMENT 'Remaining unpaid amount',
    is_partial_payment      BOOLEAN,
    days_to_pay             INT             COMMENT 'Days between bill_date and payment_date',

    -- Notices
    disconnection_notice    BOOLEAN         COMMENT 'Disconnection notice issued for this bill?',
    reconnection_fee        DOUBLE,

    -- Meter
    meter_number            STRING,
    meter_type              STRING          COMMENT 'conventional, smart_meter, prepaid_meter, tod_meter, net_meter',

    -- Metadata
    _etl_loaded_at          TIMESTAMP
)
PARTITIONED BY (bill_month STRING COMMENT 'YYYY-MM partition key')
STORED AS PARQUET
LOCATION 's3://power-cesc-datalake/gold/utility_billing/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
