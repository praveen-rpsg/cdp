# CESC integration in Unify360

This document is an onboarding guide to the Power CESC integration in Unify360. It explains the data model, identity rules, backend and frontend paths, segmentation behavior, customer views, Communication Grouping View, testing, and operational boundaries.

The application brand code is:

```text
power_cesc
```

## 1. Integration overview

Unify360 uses CESC data for:

- Live Audience segmentation.
- CESC-specific utility, billing, account, payment, lifecycle, and communication attributes.
- CESC customer search and the single-customer view.
- An experimental Communication Grouping View that estimates logical communication entities.

The integration is brand-gated. CESC behavior must be selected by `brand_code == "power_cesc"`. CESC logic must not become the default path for Spencer's, Nature's Basket, Corporate, or any other brand.

The broad data flow is:

```text
CESC warehouse / source data
        |
        v
Gold and serving relations
        |
        +--> backend catalog and compiler
        |       |
        |       +--> estimate, preview, summary APIs
        |       +--> attribute-values API
        |       +--> customer search and single view
        |
        v
Frontend brand selector, attribute picker, rule builder, and audience panels
```

The serving view is the entity-grain segmentation surface. Detail-grain tables, such as the monthly billing table, must be queried with correlated subqueries when a rule needs historical or repeated rows.

## 2. CESC data model

### 2.1 Serving view

The primary CESC segmentation relation is:

```text
cesc_serving.customer_360_seg_v
```

It is treated by the active PostgreSQL compiler as a one-row-per-CESC-entity serving view. Representative columns include:

```text
cesc_entity_id
customer_id
consumer_no
customer_name
mobile_number
email_id
consumer_account_status
net_amount
bill_type
payment_channel
payment_method
first_bill_date
tenure_days
tenure_months
monthly_consumption
average_consumption
avg_consumption_3m
avg_consumption_6m
avg_consumption_12m
consumption_bucket
outstanding_burden_ratio
```

The view also has a `bill_ac_month` value, but that value is the latest available month for an entity. It is not the correct source for asking whether an entity had a bill in an arbitrary historical month.

### 2.2 Gold customer relation

The catalog uses this CESC Gold customer relation for ordinary profile attributes:

```text
silver_gold.cesc_customer_360
```

The active segmentation compiler normally queries the serving view, so upstream Gold changes must be reflected in the serving layer before they are expected in audience results.

### 2.3 Monthly billing relation

Historical accounting-month filtering uses:

```text
silver_gold.cesc_customer_monthly
```

Important columns and types are:

```text
cesc_entity_id  text
bill_ac_month   text       -- fixed-width YYYYMM
month_start     date
month_seq       integer
```

The table is at CESC entity/month grain and has a unique index on:

```text
(cesc_entity_id, bill_ac_month)
```

Do not replace this source with `cesc_serving.customer_360_seg_v.bill_ac_month`; that would miss entities whose bill exists in an earlier month.

### 2.4 Grouping decision relation

The experimental Communication Grouping View uses:

```text
cesc_silver.cesc_bill_only_grouping_decision_20261027
```

Eligible decision values are:

```text
PERSON_COMMUNICATION_GROUP
PERSON_COMMUNICATION_GROUP_EMAIL
```

The decision relation is joined to the serving view using `consumer_no`. Its `person_communication_group_id` supplies the logical communication-group identifier.

## 3. Identity and counting contract

### 3.1 CESC identity

For CESC segmentation, the identity column is:

```text
p.cesc_entity_id
```

The ordinary CESC count shape is:

```sql
SELECT COUNT(DISTINCT p.cesc_entity_id) AS audience_count
FROM cesc_serving.customer_360_seg_v p
WHERE ...;
```

It is used for:

- audience counts;
- segment output;
- segment membership operations;
- CESC preview rows;
- customer search and single-customer lookup.

The output aliases are part of the API contract:

- count queries return `audience_count`;
- entity and preview rows return `customer_id`;
- the underlying CESC identity remains `cesc_entity_id`.

Other brands use their own identity conventions. Do not copy a Spencer's `unified_id` join into the CESC compiler.

### 3.2 Customer search and single-customer view

CESC customer search and single-customer logic are in:

```text
backend/app/services/customer_view/service.py
```

The CESC branch searches the serving view, returns `cesc_entity_id` as the identity, and exposes CESC-specific customer data. The frontend presentation is in:

```text
frontend/src/components/customer-view/CustomerSingleView.tsx
```

It switches behavior based on `brand_code === "power_cesc"`. Customer view is separate from audience compilation; changing segmentation SQL does not automatically change customer search or single-customer rendering.

## 4. Backend integration

### 4.1 Brand registration

The CESC brand is registered in:

```text
backend/app/core/config.py
backend/app/api/v1/brands.py
backend/app/main.py
```

Its public code is `power_cesc` and its display name is CESC.

### 4.2 PostgreSQL compiler

The active compiler is:

```text
backend/app/services/query_engine/pg_compiler.py
```

For `power_cesc`, it:

- uses `cesc_serving.customer_360_seg_v` as the profile source;
- uses `p.cesc_entity_id` for counts and output identity;
- avoids Spencer's behavioral joins for ordinary CESC attributes;
- resolves ordinary `cesc.*` attributes against the CESC serving alias `p`;
- provides special compiler paths for CESC attributes whose data lives at another grain.

The normal compiler methods are:

```text
PgCompiler.compile()
PgCompiler.compile_count()
PgCompiler.compile_preview()
PgCompiler.compile_summary()
```

The service factory in `backend/app/services/segmentation/service.py` selects the CESC compiler when the brand code is `power_cesc`.

### 4.3 Segmentation API flow

Request and response models are in:

```text
backend/app/schemas/api_schemas.py
```

Segmentation routes are in:

```text
backend/app/api/v1/segments.py
```

The estimate flow is:

```text
Segment Builder rule tree
  -> POST /api/v1/segments/estimate
  -> AudienceEstimateRequest
  -> SegmentationService.estimate_audience_size()
  -> _make_compiler("power_cesc")
  -> PgCompiler SQL
  -> PostgreSQL CESC source
```

Preview and summary use corresponding service methods and retain their existing response contracts. Live Audience is the ordinary path when `communication_group_view` is false.

## 5. Attribute catalog and frontend

### 5.1 Catalog

The central catalog is:

```text
backend/app/schemas/profile_attributes.py
```

A CESC attribute should normally have:

- a `cesc.*` key;
- a user-facing label;
- a data type and operator set;
- `applicable_brands=["power_cesc"]`;
- the correct CESC source table.

The catalog contains CESC identity, utility billing, lifecycle, consumption, payment, arrear, and channel attributes. Examples include:

```text
cesc.cesc_entity_id
cesc.customer_id
cesc.consumer_no
cesc.consumer_account_status
cesc.net_amount
cesc.first_bill_date
cesc.tenure_days
cesc.bill_type
cesc.bill_ac_month
cesc.payment_channel
cesc.payment_method
```

Brand applicability prevents CESC-only attributes from appearing for unrelated brands.

### 5.2 Segment Builder

The main frontend files are:

```text
frontend/src/components/segment-builder/SegmentBuilder.tsx
frontend/src/components/segment-builder/ConditionRow.tsx
frontend/src/store/segmentStore.ts
```

The picker reads the catalog and condition rows use catalog metadata to choose the input. String attributes with database-backed values load:

```text
/api/v1/segments/attributes/{attribute_key}/values?brand_code=power_cesc
```

The existing string control supports selectable values and multi-value `in_list` conditions. CESC-specific frontend code should be added only when the generic catalog-driven control cannot represent the attribute.

## 6. Bill AC Month integration

### 6.1 Meaning

`cesc.bill_ac_month` is the CESC attribute labelled **Bill AC Month**. It is a string in `YYYYMM` format, for example `202602`.

The rule:

```text
cesc.bill_ac_month equals 202602
```

means:

> include every CESC entity with at least one row in `silver_gold.cesc_customer_monthly` whose `bill_ac_month` is `202602`.

The result is counted in entities, not bill rows.

### 6.2 Compiler SQL

The CESC compiler intercepts this attribute only for `power_cesc` and emits:

```sql
EXISTS (
    SELECT 1
    FROM silver_gold.cesc_customer_monthly cm
    WHERE cm.cesc_entity_id = p.cesc_entity_id
      AND cm.bill_ac_month = '202602'
)
```

This correlated `EXISTS` preserves one row per serving entity and avoids monthly-table join fan-out.

Values are validated as six digits and quoted as text. An incoming integer `202602` must compile as `'202602'`, not as an integer literal.

### 6.3 Operators

| Operator | Entity-level meaning |
|---|---|
| `equals` | Has a bill in the selected month. |
| `in_list` | Has a bill in any selected month. |
| `not_equals` | Has no bill in the selected month. |
| `not_in_list` | Has no bill in any selected month in the list. |
| `before`, `after` | Compare fixed-width `YYYYMM` text. |
| `greater_than`, `less_than` | Compare fixed-width month text. |
| `greater_than_or_equal`, `less_than_or_equal` | Inclusive fixed-width month comparison. |
| `between` | Inclusive month range. |
| `not_between` | No bill in the inclusive month range. |
| `exists` | Has at least one monthly row. |
| `not_exists` | Has no monthly row. |

Positive rules use `EXISTS`; negative rules use `NOT (EXISTS (...))`. Condition-level negation wraps the complete monthly predicate.

### 6.4 Values endpoint

For `power_cesc` and `cesc.bill_ac_month`, the explicit values query is equivalent to:

```sql
SELECT DISTINCT bill_ac_month::TEXT AS val
FROM silver_gold.cesc_customer_monthly
WHERE bill_ac_month ~ '^[0-9]{6}$'
ORDER BY bill_ac_month DESC;
```

The catalog uses an empty example-value list, so an empty database result does not fall back to misleading hardcoded months. Table and column identifiers are code-defined, not taken from request input.

### 6.5 Group View boundary

`cesc.bill_ac_month` is explicitly unsupported in Communication Grouping View. Historical monthly group semantics have not been defined; the attribute must not silently fall back to the latest-month column on the serving view.

### 6.6 Current data snapshot

One verified database snapshot contained:

```text
Valid monthly value:                    202602
Distinct monthly entities:             2,728,391
Distinct serving entities:             2,980,310
Direct not-equals count:                 251,919
```

The arithmetic is:

```text
2,980,310 - 2,728,391 = 251,919
```

The values endpoint returned `202602`, and the API estimate for `equals 202602` returned `2,728,391` with the correlated EXISTS query. These values describe that database snapshot; they are not permanent constants.

## 7. Communication Grouping View

### 7.1 Intended goal

Communication Grouping View answers a different question from Live Audience:

> How many logical communication targets remain after entities belonging to the same eligible CESC communication group are treated as one target?

The intended logical keys are:

```text
G:<person_communication_group_id>   grouped entities
E:<cesc_entity_id>                  ungrouped entity
```

The prefixes prevent group IDs and entity IDs from colliding.

### 7.2 Existing implementation

The repository contains `CescGroupViewPgCompiler` in:

```text
backend/app/services/query_engine/pg_compiler.py
```

Request-time selection is wired through:

```text
backend/app/services/segmentation/service.py
backend/app/schemas/api_schemas.py
backend/app/api/v1/segments.py
frontend/src/store/segmentStore.ts
frontend/src/components/segment-builder/SegmentBuilder.tsx
```

Its rough query shape is:

```sql
WITH member_map AS (
    SELECT consumer_no, person_communication_group_id
    FROM cesc_silver.cesc_bill_only_grouping_decision_20261027
    WHERE grouping_decision IN (
        'PERSON_COMMUNICATION_GROUP',
        'PERSON_COMMUNICATION_GROUP_EMAIL'
    )
      AND person_communication_group_id IS NOT NULL
    GROUP BY consumer_no, person_communication_group_id
)
SELECT CASE
         WHEN m.person_communication_group_id IS NOT NULL
           THEN 'G:' || m.person_communication_group_id
         ELSE 'E:' || p.cesc_entity_id
       END AS customer_id
FROM cesc_serving.customer_360_seg_v p
LEFT JOIN member_map m USING (consumer_no)
GROUP BY CASE
           WHEN m.person_communication_group_id IS NOT NULL
             THEN 'G:' || m.person_communication_group_id
           ELSE 'E:' || p.cesc_entity_id
         END
```

The implementation also has a runtime validation hook intended to detect a consumer mapping to multiple groups.

### 7.3 Rough grouping estimates

Earlier diagnostic checks reported approximately:

```text
Serving entities:                         2,980,310
Entities assigned to eligible groups:       412,141
Eligible groups:                            338,628
Rough no-condition grouped count:         2,906,797
Maximum group size:                              11
Entities in more than one group:                 0
```

The rough calculation is:

```text
2,980,310 - 412,141 + 338,628 = 2,906,797
```

These are diagnostic estimates for one data snapshot. They must be recalculated whenever the serving view or decision relation changes.

### 7.4 Current status: incomplete

Communication Grouping View exists, but it is not properly implemented as a production feature. Known limitations include:

- API/frontend wiring has not consistently exposed an independent Group View count instead of the ordinary Live estimate.
- Grouped queries are expensive at the current data volume.
- Aggregate semantics are not complete for every CESC attribute.
- Historical monthly attributes are intentionally unsupported.
- Set Operations, Split, and Rank & Split are unavailable while Group View is enabled.
- Correctness depends on the integrity and uniqueness of the decision mapping.
- Saved-segment and downstream audience semantics need a separate product contract.

Do not use the rough grouping number as a production delivery count. Do not add an attribute to Group View without defining its group-level semantics and testing the result against seeded data.

## 8. Customer-view integration

The CESC customer-view path is separate from the segment compiler:

```text
backend/app/services/customer_view/service.py
frontend/src/components/customer-view/CustomerSingleView.tsx
```

The backend CESC branch searches the serving view, returns `cesc_entity_id` as the identity, and maps CESC-specific fields into the customer-view response. The frontend uses the CESC brand code to render CESC-specific sections and labels.

## 9. Testing and diagnostics

Focused CESC tests are in:

```text
backend/tests/test_cesc_pg_compiler.py
backend/tests/test_cesc_bill_ac_month.py
backend/tests/test_cesc_group_view_compiler.py
```

The Bill AC Month test suite covers catalog metadata, brand applicability, EXISTS correlation, text quoting for string and integer inputs, negative operators, ordered/between operators, and Group View rejection.

The focused test command is:

```powershell
docker compose run --rm backend sh -c "pip install -q pytest pytest-asyncio && pytest tests/test_cesc_bill_ac_month.py -q"
```

One verified run produced:

```text
......                                                                   [100%]
6 passed in 0.93s
```

The broader read-only grouping diagnostic script is:

```text
backend/_group_checks.py
```

It checks serving identity shape, grouping categories, decision-to-view coverage, group sizes, tenure and consumption ranges, and burden-ratio relationships. It is a diagnostic script, not a migration and not an application startup dependency.

Useful direct checks include:

```sql
SELECT COUNT(*) AS rows_total,
       COUNT(DISTINCT cesc_entity_id) AS distinct_entities
FROM cesc_serving.customer_360_seg_v;

SELECT bill_ac_month,
       COUNT(DISTINCT cesc_entity_id) AS entities
FROM silver_gold.cesc_customer_monthly
GROUP BY bill_ac_month
ORDER BY bill_ac_month;

SELECT grouping_decision,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT consumer_no) AS distinct_consumers,
       COUNT(DISTINCT person_communication_group_id) AS distinct_groups
FROM cesc_silver.cesc_bill_only_grouping_decision_20261027
GROUP BY grouping_decision
ORDER BY grouping_decision;
```

## 10. Development and troubleshooting

After changing mounted backend source files in the Docker development setup:

```powershell
docker compose restart backend
```

Restart the frontend separately when needed:

```powershell
docker compose restart frontend
```

When inspecting generated SQL for a CESC rule, confirm:

1. The source is `cesc_serving.customer_360_seg_v`.
2. Counts use `COUNT(DISTINCT p.cesc_entity_id)`.
3. Preview rows use the `customer_id` alias.
4. Monthly rules use `EXISTS` against `silver_gold.cesc_customer_monthly`.
5. A Live request is not accidentally using Group View.

Common mistakes:

| Mistake | Consequence | Correct approach |
|---|---|---|
| Using `unified_id` for CESC audience identity | Missing or incorrect matches | Use `cesc_entity_id`. |
| Joining monthly rows directly | Duplicate rows and inflated counts | Use correlated `EXISTS`. |
| Filtering the latest-month serving column | Historical billers are missed | Use `cesc_customer_monthly`. |
| Coercing `202602` to an integer | Text/integer comparison failure | Validate and quote month values. |
| Adding CESC keys to shared maps without a brand gate | Other brands can resolve to CESC data | Gate on `power_cesc`. |
| Treating Group View as Live Audience | Wrong logical-entity counts | Keep the paths separate. |
| Adding a Group View attribute without aggregate semantics | Silent incorrect grouping | Reject it until semantics exist. |

## 11. Change boundaries and source map

Preserve these invariants:

1. CESC behavior is selected only for `power_cesc`.
2. CESC audience identity is `cesc_entity_id`.
3. Count and preview aliases remain `audience_count` and `customer_id`.
4. Historical monthly filters remain entity-level EXISTS predicates.
5. CESC changes do not alter Spencer's, Nature's Basket, or Corporate SQL.
6. Bill AC Month requires no DDL, backfill, or identity-data rewrite.
7. Group View remains separate until its API, UI, performance, and semantics are completed.

Relevant source files:

| Area | File |
|---|---|
| Brand enum/configuration | `backend/app/core/config.py` |
| Brand API listing | `backend/app/api/v1/brands.py` |
| Attribute catalog | `backend/app/schemas/profile_attributes.py` |
| Request/response schemas | `backend/app/schemas/api_schemas.py` |
| PostgreSQL compiler | `backend/app/services/query_engine/pg_compiler.py` |
| Segmentation service | `backend/app/services/segmentation/service.py` |
| Segmentation routes | `backend/app/api/v1/segments.py` |
| Customer search/single view | `backend/app/services/customer_view/service.py` |
| Segment Builder | `frontend/src/components/segment-builder/SegmentBuilder.tsx` |
| Condition rendering/values | `frontend/src/components/segment-builder/ConditionRow.tsx` |
| Audience state | `frontend/src/store/segmentStore.ts` |
| CESC compiler tests | `backend/tests/test_cesc_pg_compiler.py` |
| Bill AC Month tests | `backend/tests/test_cesc_bill_ac_month.py` |
| Group View tests | `backend/tests/test_cesc_group_view_compiler.py` |
| Grouping diagnostics | `backend/_group_checks.py` |
