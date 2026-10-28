SELECT bill_source_key, COUNT(*)
FROM "cdp_meta"."cesc_silver"."cesc_bill_spine"
GROUP BY bill_source_key
HAVING COUNT(*) > 1