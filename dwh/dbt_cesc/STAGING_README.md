# CESC Normalization / Staging Layer

## Purpose

The CESC staging layer converts source-faithful Bronze data into normalized,
query-friendly dbt views without modifying Bronze data or performing identity
resolution.

Pipeline:

Bronze -> CESC Staging / Normalization -> Identity Resolution -> UNIFY360 / Silver

Identity resolution is explicitly out of scope for this layer.

---

## CESC dbt Project

Project:
- dwh/dbt_cesc

Database:
- PostgreSQL database: cdp_meta
- Native PostgreSQL host: localhost:5432
- Docker access: host.docker.internal:5432

dbt:
- dbt-core 1.8.7
- dbt-postgres 1.8.2

Staging schema:
- staging

All CESC staging models are materialized as views.

---

## Source Tables

Bronze sources:

- bronze.cesc_cih_master
- bronze.cesc_lifetime_attributes
- bronze.cesc_bill_monthly

Bronze remains source-faithful and is not modified by staging models.

---

## Staging Models

### 1. stg_cesc_cih

One row per CESC CIH source record.

Responsibilities:
- trim and normalize scalar text fields
- normalize names for canonical comparison representation
- normalize email representation
- validate email format
- normalize CIH mobile using brand_id_s
- preserve mobile_no_s separately as source data
- normalize is_mobile_b when its source encoding is understood
- preserve DND source value without inventing semantic mappings
- preserve all *_ss fields in their raw staging representation
- retain ingestion lineage

Technical row key:
- cih_source_row_key

R1:
- r1_source_ref is retained as the Resulticks source reference
- R1 is not a UNIFY360 identity key
- R1 is not assumed to be unique

---

## Phone Normalization

### CIH

brand_id_s is used as the canonical CIH mobile source because the observed
CIH data contains a consistent 10-digit representation.

Observed data:
- 2,191,124 of 2,191,125 CIH rows had a 10-digit brand_id_s
- 1 row was blank/null

mobile_no_s is retained as mobile_no_source_raw.

mobile_no_s is NOT reconstructed into a canonical phone because a large
portion of the source is represented in scientific notation and the displayed
representation is lossy.

Canonical CIH rule:

- exactly 10 digits -> mobile_normalized
- anything else -> NULL

---

### Bills

Bill mobile_no values were observed as:

- 3,450,193 clean 10-digit values
- 280,781 missing values
- no observed scientific-notation values

Canonical bill rule:

- exactly 10 digits -> mobile_normalized
- blank/missing -> NULL
- other malformed values -> NULL

Bill staging exposes:
- mobile_raw
- mobile_normalized
- mobile_valid_flag

CIH staging exposes:
- mobile_raw
- mobile_normalized
- mobile_no_source_raw

---

## Email Normalization

Email normalization is deliberately conservative.

Fields:
- email_raw
- email_normalized
- email_valid_flag

Rules:

1. Trim surrounding whitespace.
2. Blank values become NULL.
3. Valid-looking email values are lowercased.
4. Values failing the accepted email shape are not treated as matching-ready.
5. Raw source values are retained.

Current CIH results:
- 1,337,954 valid
- 170 invalid
- 853,001 missing

Current February bill results:
- 2,243,648 valid
- 201 invalid
- 1,487,125 missing

Invalid does not mean the email address is known to be nonexistent.
It only means the value does not satisfy the staging format rule.

---

## Missing Values

Normalization does not invent missing information.

For a normalized field:

valid source value -> normalized value

blank / missing -> NULL

malformed nonblank source value -> preserve raw source value and set the
normalized representation to NULL where the field has a validity rule.

Records are not removed because a contact field is missing.

Missing phone/email values must not be replaced with:
- consumer number
- customer ID
- another customer's value
- a historical value
- a guessed value

---

## Consumer Number History

Model:
- staging.stg_cesc_cih_consumer_numbers

Source:
- consumer_no_ss

This model independently parses the multi-valued consumer_no_ss field.

It does NOT pair consumer numbers with bill months or any other *_ss field.

One CIH source record may have:
- one consumer number
- multiple consumer numbers
- no consumer number

The normalized model contains:
- cih_source_row_key
- r1_source_ref
- consumer_number_ordinal
- consumer_no
- ingestion lineage

The technical parent reference is cih_source_row_key.

---

## Billing Month History

Model:
- staging.stg_cesc_cih_bill_months

Source:
- bill_ac_month_ss

This model independently parses the multi-valued bill_ac_month_ss field.

It does NOT pair billing months with:
- consumer numbers
- regional offices
- consumer types
- supply types
- other *_ss fields

A CIH record can naturally have many historical billing months even when it
has only one consumer number.

The normalized model contains:
- cih_source_row_key
- r1_source_ref
- bill_month_ordinal
- bill_ac_month
- ingestion lineage

Observed history:
- 69,679,512 parsed bill-month rows
- 37 distinct billing months
- earliest observed month: 202207
- latest observed month: 202510

---

## Other *_ss Fields

CESC CIH contains several additional multi-valued source fields, including:

- regional_office_ss
- consumer_type_ss
- road_code_ss
- premises_no_ss
- lamp_post_no_ss
- event_bill_ac_month_ss
- bill_type_ss
- consumer_account_status_ss
- ebill_status_ss
- supply_mode_ss
- supply_type_ss
- consumer_status_code_ss

These remain preserved in stg_cesc_cih.

They are NOT assumed to be positionally aligned with one another.

They should only be relationalized into additional child models when a
specific downstream requirement justifies doing so.

Do not create artificial tuples across independently varying arrays.

---

## Lifetime Staging

Model:
- staging.stg_cesc_lifetime

One row per lifetime source record.

Source attributes are already physically typed in Bronze.

The staging model:
- provides normalized semantic names
- preserves source lineage
- does not invent monthly partitioning
- does not perform identity resolution

R1 is unique in the current lifetime dataset and is retained as a source
reference.

Current row count:
- Bronze: 2,596,552
- Staging: 2,596,552

---

## Bill Staging

Model:
- staging.stg_cesc_bill

One row per monthly bill source record.

The model:
- trims text identifiers and categorical fields
- preserves numeric and date values already typed in Bronze
- normalizes bill email
- normalizes bill phone
- adds email_valid_flag
- adds mobile_valid_flag
- preserves ingestion lineage

Current row count:
- Bronze: 3,730,974
- Staging: 3,730,974

No aggregation or identity assignment occurs here.

---

## Row Preservation

Parent staging models must preserve Bronze row counts.

Current validated counts:

CIH:
- Bronze: 2,191,125
- Staging: 2,191,125

Bills:
- Bronze: 3,730,974
- Staging: 3,730,974

Lifetime:
- Bronze: 2,596,552
- Staging: 2,596,552

The two history models intentionally expand row counts because they represent
multi-valued source observations.

---

## Technical Lineage

cih_source_row_key is a technical staging-row identifier for the CIH source
record.

It is derived from source-row content and ingestion lineage.

It is NOT:
- a customer ID
- a unified ID
- an identity-resolution key
- a replacement for R1 in the source system

R1 is retained only as a source-system reference.

---

## Identity Resolution Boundary

The staging layer must NOT perform:

- deduplication
- identity matching
- identity merging
- unified ID generation
- confidence scoring
- fuzzy name matching
- survivorship decisions
- person-level consolidation
- consumer-number-to-person assignment

Those responsibilities belong to the later identity-resolution layer.

---

## Validation Completed

The CESC staging layer has been validated for:

- dbt compilation
- database connectivity
- parent row preservation
- CIH technical-key uniqueness
- consumer-number parsing
- bill-month parsing
- required non-null child fields
- accepted boolean values
- Bronze row-count preservation

Current automated dbt tests:
- 10 passed
- 0 warnings
- 0 errors
- 0 skipped

---

## Important Design Principles

1. Bronze remains untouched.
2. Normalize representation, do not invent missing information.
3. Preserve raw/source evidence.
4. Treat phone/email validity as syntactic/format validation, not identity truth.
5. Keep independently varying *_ss histories independent.
6. Use technical lineage keys only for data engineering purposes.
7. Keep identity resolution completely separate from normalization.
8. Do not copy Spencer's identity assumptions into CESC.

