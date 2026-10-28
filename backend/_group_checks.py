import os
import psycopg

conninfo = "host={h} port={p} dbname={d} user={u} password={pw}".format(
    h=os.getenv("PG_HOST", "localhost"),
    p=os.getenv("PG_PORT", "5432"),
    d=os.getenv("PG_DB", "cdp_meta"),
    u=os.getenv("PG_USER", "cdp"),
    pw=os.getenv("PG_PASSWORD", "cdp"),
)

V = "cesc_serving.customer_360_seg_v"
D = "cesc_silver.cesc_bill_only_grouping_decision_20261027"
ELIG = "('PERSON_COMMUNICATION_GROUP','PERSON_COMMUNICATION_GROUP_EMAIL')"

QUERIES = {
"01_view_identity": f"""
SELECT count(*) AS rows_total,
       count(DISTINCT cesc_entity_id) AS distinct_entity,
       count(DISTINCT unified_id) AS distinct_unified,
       count(*) FILTER (WHERE unified_id = cesc_entity_id) AS unified_eq_entity,
       count(*) FILTER (WHERE unified_id = customer_id) AS unified_eq_customer,
       count(DISTINCT consumer_no) AS distinct_consumer_no,
       count(*) FILTER (WHERE consumer_no IS NULL) AS null_consumer_no,
       count(*) FILTER (WHERE latest_month_consumer_numbers > 1) AS multi_consumer_entities
FROM {V}""",
"02_decision_categories": f"""
SELECT grouping_decision, count(*) AS rows_n,
       count(DISTINCT consumer_no) AS distinct_consumer,
       count(DISTINCT person_communication_group_id) AS distinct_person_groups,
       count(*) FILTER (WHERE person_communication_group_id IS NULL) AS null_person_group,
       count(DISTINCT organisation_group_id) AS distinct_org_groups
FROM {D} GROUP BY 1 ORDER BY 1""",
"03_decision_to_view_coverage": f"""
SELECT d.grouping_decision, count(*) AS joined_rows,
       count(DISTINCT d.consumer_no) AS decision_consumers,
       count(DISTINCT d.consumer_no) FILTER (WHERE v.unified_id IS NOT NULL) AS matched_consumers,
       count(DISTINCT v.unified_id) AS matched_entities
FROM {D} d LEFT JOIN {V} v ON v.consumer_no = d.consumer_no
GROUP BY 1 ORDER BY 1""",
"04_group_shape": f"""
WITH t AS (
  SELECT d.person_communication_group_id AS g, v.unified_id AS u
  FROM {D} d JOIN {V} v ON v.consumer_no = d.consumer_no
  WHERE d.grouping_decision IN {ELIG} AND d.person_communication_group_id IS NOT NULL
), a AS (SELECT g, count(DISTINCT u) AS n FROM t GROUP BY g),
   b AS (SELECT u, count(DISTINCT g) AS m FROM t GROUP BY u)
SELECT (SELECT count(*) FROM a) AS groups_n,
       (SELECT max(n) FROM a) AS max_entities_per_group,
       (SELECT count(*) FROM a WHERE n = 1) AS single_entity_groups,
       (SELECT count(*) FROM b) AS grouped_entities,
       (SELECT max(m) FROM b) AS max_groups_per_entity,
       (SELECT count(*) FROM b WHERE m > 1) AS entities_in_multiple_groups""",
"05_tenure_bucket_ranges": f"""
SELECT tenure_bucket, count(*) AS n, min(tenure_days) AS min_days, max(tenure_days) AS max_days
FROM {V} GROUP BY 1 ORDER BY min(tenure_days)""",
"06_consumption_bucket_ranges": f"""
SELECT consumption_bucket, count(*) AS n,
       min(monthly_consumption) AS mc_min, max(monthly_consumption) AS mc_max,
       min(average_consumption) AS ac_min, max(average_consumption) AS ac_max,
       min(avg_consumption_3m) AS a3_min, max(avg_consumption_3m) AS a3_max
FROM {V} GROUP BY 1 ORDER BY min(monthly_consumption)""",
"07_burden_ratio_formula": f"""
SELECT count(*) FILTER (WHERE outstanding_burden_ratio IS NOT NULL) AS ratio_not_null,
  count(*) FILTER (WHERE abs(outstanding_burden_ratio - arrear_amount/NULLIF(net_amount,0)) < 0.0001) AS eq_arrear_over_net,
  count(*) FILTER (WHERE abs(outstanding_burden_ratio - arrear_amount/NULLIF(gross_amount,0)) < 0.0001) AS eq_arrear_over_gross,
  count(*) FILTER (WHERE abs(outstanding_burden_ratio - arrear_amount/NULLIF(net_amount+arrear_amount,0)) < 0.0001) AS eq_arrear_over_net_plus_arrear,
  count(*) FILTER (WHERE abs(outstanding_burden_ratio - 100*arrear_amount/NULLIF(net_amount,0)) < 0.01) AS eq_pct_arrear_over_net
FROM {V}""",
"08_burden_ratio_sample": f"""
SELECT outstanding_burden_ratio, arrear_amount, net_amount, gross_amount
FROM {V} WHERE outstanding_burden_ratio IS NOT NULL AND outstanding_burden_ratio > 0 LIMIT 5""",
}

with psycopg.connect(conninfo) as conn:
    for name, sql in QUERIES.items():
        print("=== " + name)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                print(" | ".join(c.name for c in cur.description))
                for row in cur.fetchall():
                    print(" | ".join(str(x) for x in row))
        except Exception as e:
            conn.rollback()
            print("ERROR: " + str(e))
        print()
