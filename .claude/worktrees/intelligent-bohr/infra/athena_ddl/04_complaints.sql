-- =============================================================================
-- 5. COMPLAINTS — Service requests and complaints (Power CESC primarily)
-- =============================================================================
-- Captures the full lifecycle of each complaint/service request.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS complaints (
    complaint_id            STRING          COMMENT 'Unique complaint/SR ID',
    customer_id             STRING          COMMENT 'FK to customers',
    account_number          STRING          COMMENT 'Utility account number',

    -- Complaint details
    complaint_type          STRING          COMMENT 'power_outage, voltage_fluctuation, meter_fault, billing_dispute, new_connection, load_enhancement, name_transfer, theft_report, street_light, safety_hazard, general_enquiry',
    complaint_sub_type      STRING,
    complaint_description   STRING,
    priority                STRING          COMMENT 'low, medium, high, critical',
    severity                STRING          COMMENT 'minor, major, critical',

    -- Channel of origin
    channel                 STRING          COMMENT 'call_center, online_portal, mobile_app, whatsapp, walk_in, email, social_media, ivr',

    -- Status lifecycle
    status                  STRING          COMMENT 'open, in_progress, assigned, resolved, closed, escalated, reopened',
    created_at              TIMESTAMP,
    acknowledged_at         TIMESTAMP,
    assigned_at             TIMESTAMP,
    assigned_to             STRING          COMMENT 'Team/technician assigned',
    resolved_at             TIMESTAMP,
    closed_at               TIMESTAMP,
    resolution_time_hours   DOUBLE          COMMENT 'Hours from creation to resolution',

    -- Escalation
    is_escalated            BOOLEAN,
    escalation_level        INT             COMMENT '1, 2, 3 (higher = more escalated)',
    escalated_at            TIMESTAMP,
    escalation_reason       STRING,

    -- Repeat / Related
    is_repeat_complaint     BOOLEAN         COMMENT 'Same issue within 30 days of prior complaint',
    related_complaint_id    STRING          COMMENT 'ID of the prior related complaint',

    -- Resolution
    resolution_type         STRING          COMMENT 'fixed_on_site, remote_fix, billing_adjustment, no_action_needed, referred',
    resolution_notes        STRING,
    is_first_contact_resolution BOOLEAN,

    -- Satisfaction (post-resolution survey)
    survey_sent             BOOLEAN,
    survey_responded        BOOLEAN,
    satisfaction_rating     STRING          COMMENT 'very_dissatisfied, dissatisfied, neutral, satisfied, very_satisfied',
    satisfaction_score      INT             COMMENT '1-5 scale',

    -- NLP / Sentiment (computed by ML pipeline)
    complaint_sentiment     STRING          COMMENT 'very_negative, negative, neutral, positive',
    sentiment_score         DOUBLE          COMMENT '-1.0 to 1.0',

    -- Location
    complaint_area          STRING          COMMENT 'Service area / distribution zone',
    complaint_pincode       STRING,
    feeder_id               STRING          COMMENT 'Electrical feeder (for outage correlation)',

    -- Outage-specific (for power_outage complaints)
    outage_start            TIMESTAMP,
    outage_end              TIMESTAMP,
    outage_duration_hours   DOUBLE,
    affected_customers_count INT,
    is_planned_outage       BOOLEAN,

    -- Regulatory
    is_regulatory_complaint BOOLEAN         COMMENT 'Filed with WBERC / consumer forum',
    regulatory_reference    STRING,

    -- Metadata
    _etl_loaded_at          TIMESTAMP
)
PARTITIONED BY (complaint_month STRING COMMENT 'YYYY-MM partition key')
STORED AS PARQUET
LOCATION 's3://power-cesc-datalake/gold/complaints/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
