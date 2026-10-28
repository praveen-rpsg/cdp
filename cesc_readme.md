# CESC implementation notes

## Purpose

This document records the CESC-specific work currently present in this repository. The brand code is `power_cesc`, and the implementation is intended to keep CESC identity and billing segmentation separate from Spencer's, Nature's Basket, and Corporate behavior.

The main objectives are:

1. Compile CESC Live Audience rules against the CESC serving view using `cesc_entity_id` as the audience identity.
2. Expose CESC profile and billing attributes in the catalog and frontend picker.
3. Support filtering CESC customers by the accounting month in which they had a bill.
4. Provide an experimental Communication Grouping View that estimates logical communication entities instead of individual customer entities.

The Bill AC Month feature is the focused, implemented CESC monthly-billing feature. Communication Grouping View exists as a separate experimental path, but it is not considered properly implemented or production-ready; see the dedicated section below.

## CESC data model and identity

The serving source used by the CESC compiler is:

```text
cesc_serving.customer_360_seg_v
```

The ordinary CESC audience unit is the distinct value of:

```text
cesc_serving.customer_360_seg_v.cesc_entity_id
```

The normal CESC count shape is therefore:

```sql
SELECT COUNT(DISTINCT p.cesc_entity_id) AS audience_count
FROM cesc_serving.customer_360_seg_v p
WHERE ...;
```

For compiled preview results, the output identity alias remains `customer_id`, while the underlying CESC identity is `p.cesc_entity_id`.

The CESC compiler is selected by `brand_code == 'power_cesc'`. Other brands continue to use their existing compiler paths and identity columns. The CESC brand configuration points the profile, behavioral, identity-graph, location, and transaction references at the CESC serving source instead of Spencer's or Nature's Basket sources.

## CESC catalog

CESC attributes are declared in `backend/app/schemas/profile_attributes.py` with `applicable_brands=["power_cesc"]`. The catalog includes identity, utility billing, lifecycle, consumption, payment, arrear, and channel attributes such as:

- `cesc.cesc_entity_id`
- `cesc.customer_id`
- `cesc.consumer_no`
- `cesc.consumer_account_status`
- `cesc.net_amount`
- `cesc.first_bill_date`
- `cesc.tenure_days`
- `cesc.bill_type`
- `cesc.bill_ac_month`
- `cesc.payment_channel`
- `cesc.payment_method`

Catalog entries use CESC source tables and are filtered by applicable brand, so the CESC attributes are available for `power_cesc` without making them universal attributes for other brands.

## Bill AC Month

### Aim

`cesc.bill_ac_month` lets a CESC segment select every entity that had at least one bill in a selected accounting month. The attribute is displayed as **Bill AC Month**, is a string, and uses the fixed-width `YYYYMM` representation, for example `202602`.

### Source

The monthly source is:

```text
silver_gold.cesc_customer_monthly
```

The relevant columns are:

```text
cesc_entity_id  text
bill_ac_month   text
```

The monthly table is at CESC entity/month grain. It has a unique B-tree index on:

```text
(cesc_entity_id, bill_ac_month)
```

The implementation intentionally does not use `bill_ac_month` from `cesc_serving.customer_360_seg_v`. That serving-view value represents the latest available month for an entity and would omit entities whose bill existed in an earlier month.

### Compiler behavior

The CESC compiler intercepts only this attribute when the brand is `power_cesc`. It emits a correlated `EXISTS` subquery, preserving one row per entity in the serving view and avoiding monthly-table join fan-out:

```sql
EXISTS (
    SELECT 1
    FROM silver_gold.cesc_customer_monthly cm
    WHERE cm.cesc_entity_id = p.cesc_entity_id
      AND cm.bill_ac_month = '202602'
)
```

Values are validated as six digits and quoted as text. This is important because an incoming numeric value such as `202602` must still compile as the text literal `'202602'`; the shared numeric-looking-value coercion must not be used for this attribute.

Supported forms include:

- `equals`: entity has a bill in the selected month.
- `in_list`: entity has a bill in any selected month.
- `not_equals`: entity has no bill in the selected month.
- `not_in_list`: entity has no bill in any selected month in the list.
- `before`, `after`, `greater_than`, `less_than`, `greater_than_or_equal`, and `less_than_or_equal`: compare the fixed-width month text.
- `between` and `not_between`: compare an inclusive text range.
- `exists`: entity has at least one monthly bill row.
- `not_exists`: entity has no monthly bill row.

Negative operators use `NOT (EXISTS (...))` around the positive monthly predicate. Condition-level negation wraps the complete EXISTS result.

### Picker values

For `power_cesc` and `cesc.bill_ac_month`, the values endpoint uses an explicit CESC target:

```sql
SELECT DISTINCT bill_ac_month::TEXT AS val
FROM silver_gold.cesc_customer_monthly
WHERE bill_ac_month ~ '^[0-9]{6}$'
ORDER BY bill_ac_month DESC;
```

The catalog has `example_values=[]`, so the UI does not show fabricated example months when the database returns no values. The existing string/dropdown condition UI can load the dynamic values endpoint and use multi-select for `in_list` without a separate CESC-specific input redesign.

### Verified current data

At the time of the last database verification, the monthly table contained one valid accounting month:

```text
202602: 2,728,391 distinct CESC entities
```

The picker endpoint returned:

```json
{"attribute_key":"cesc.bill_ac_month","values":["202602"],"count":1,"source":"database"}
```

The API estimate for `cesc.bill_ac_month equals '202602'` returned `2,728,391` and generated:

```sql
SELECT COUNT(DISTINCT p.cesc_entity_id) AS audience_count
FROM cesc_serving.customer_360_seg_v p
WHERE (EXISTS (
    SELECT 1
    FROM silver_gold.cesc_customer_monthly cm
    WHERE cm.cesc_entity_id = p.cesc_entity_id
      AND cm.bill_ac_month = '202602'
))
```

The direct database count was also `2,728,391`.

The current serving view had `2,980,310` distinct entities. The direct `not_equals '202602'` count was `251,919`, satisfying:

```text
2,980,310 - 2,728,391 = 251,919
```

The dataset currently has only one monthly value, so a three-month historical comparison cannot be made from this database snapshot.

### Tests

Focused tests are in:

```text
backend/tests/test_cesc_bill_ac_month.py
```

They cover:

- catalog metadata and brand applicability;
- quoted text compilation for both string and integer inputs;
- correlation on `cesc_entity_id`;
- negative operators using `NOT (EXISTS (...))`;
- ordered/between text operators;
- rejection by the CESC Group View compiler because monthly logic is not implemented there.

The test command used was:

```powershell
docker compose run --rm backend sh -c "pip install -q pytest pytest-asyncio && pytest tests/test_cesc_bill_ac_month.py -q"
```

Observed result:

```text
......                                                                   [100%]
6 passed in 0.93s
```

## Communication Grouping View

### Aim and goal

The intended goal of Communication Grouping View is to estimate an audience in terms of logical communication entities rather than individual CESC entities. It is intended for use cases where several customer entities should be treated as one communication target, based on an eligible communication-grouping decision table.

The desired grouping logic is:

- entities with an eligible `person_communication_group_id` become one logical group;
- entities without a group remain individual logical entities;
- the result should count logical entities, not serving-view rows;
- group keys should be namespaced as `G:<group_id>` and `E:<cesc_entity_id>` to avoid collisions.

The configured decision source currently referenced by the grouping path is:

```text
cesc_silver.cesc_bill_only_grouping_decision_20261027
```

Eligible decisions are:

```text
PERSON_COMMUNICATION_GROUP
PERSON_COMMUNICATION_GROUP_EMAIL
```

### What exists

The repository contains a separate `CescGroupViewPgCompiler` and request-time service wiring. Its rough count shape uses a member map and a logical key similar to:

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

The grouping compiler also contains a runtime duplicate-map validation hook. The monthly Bill AC Month attribute is explicitly rejected by this compiler with an unsupported-in-Group-View error; monthly group-level semantics were not implemented as part of the Bill AC Month work.

### Rough grouping estimates

Earlier CESC grouping checks produced these rough estimates for the then-current data:

```text
Serving entities:                         2,980,310
Entities belonging to eligible groups:      412,141
Eligible logical groups:                    338,628
Estimated no-condition logical audience:  2,906,797
```

The arithmetic behind the rough estimate is:

```text
2,980,310 - 412,141 + 338,628 = 2,906,797
```

The earlier checks also reported a maximum group size of 11, no entity in more than one eligible group, and full decision-consumer coverage against the serving view. These are diagnostic estimates, not a product guarantee.

### Important status: not properly implemented

Communication Grouping View is not properly implemented yet. It exists in the codebase as an experimental path, but it has known integration and correctness limitations:

- it is separate from the ordinary Live Audience path and should not be treated as a replacement for Live Audience;
- the UI/API wiring has historically exposed the ordinary estimated count instead of consistently exposing a separate Group View count;
- Group View performance is materially slower than the ordinary Live count on the current data volume;
- aggregate semantics for all attributes, especially monthly attributes, are incomplete or intentionally unsupported;
- Set Operations, Split, and Rank & Split are not supported in the Group View path;
- the rough arithmetic above assumes clean one-group-per-entity mapping and should be revalidated whenever the decision table changes;
- the earlier Group View implementation and helper artifacts remain in the repository and were not deleted.

Consequently, the grouping numbers in this document are rough estimates for investigation and planning only. They should not be used as an approved production audience count until the API response, frontend rendering, aggregate semantics, and performance behavior are completed and reverified.

## Scope boundaries

The CESC-specific work is intended to preserve these boundaries:

- no changes to Spencer's behavior;
- no changes to Nature's Basket behavior;
- no changes to Corporate behavior;
- no identity-data backfill or identity-data rewrite;
- no database DDL or backfill for Bill AC Month;
- no change to existing Live Audience SQL for existing attributes as part of the monthly attribute feature;
- no Group View monthly aggregation logic.

## Relevant files

- `backend/app/schemas/profile_attributes.py` — CESC catalog declarations, including `cesc.bill_ac_month`.
- `backend/app/services/query_engine/pg_compiler.py` — CESC Live compiler identity handling, Bill AC Month EXISTS compilation, and the separate experimental Group View compiler.
- `backend/app/services/segmentation/service.py` — CESC compiler selection and explicit Bill AC Month values lookup.
- `backend/app/schemas/api_schemas.py` — request models that carry the existing communication-group-view flag.
- `backend/app/api/v1/segments.py` — segmentation API routes and request forwarding.
- `backend/tests/test_cesc_bill_ac_month.py` — focused Bill AC Month compiler tests.
- `backend/_group_checks.py` — read-only diagnostic SQL for the CESC serving view and grouping decision table.
- `frontend/src/components/segment-builder/ConditionRow.tsx` — existing dynamic attribute value loading and string dropdown behavior.
- `frontend/src/store/segmentStore.ts` — existing audience-estimate state handling.
- `frontend/src/components/segment-builder/SegmentBuilder.tsx` — existing segment-builder UI wiring.

## Operational note

After changing backend source files in the Docker development setup, restart the backend service to reload the application:

```powershell
docker compose restart backend
```

No database restart, DDL, migration, or backfill is required for Bill AC Month.
