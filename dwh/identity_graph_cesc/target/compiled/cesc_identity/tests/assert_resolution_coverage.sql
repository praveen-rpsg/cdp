SELECT b.bill_source_key
FROM "cdp_meta"."cesc_silver"."cesc_bill_spine" b
LEFT JOIN "cdp_meta"."cesc_silver"."cesc_identity_resolution" r
    ON b.bill_source_key = r.bill_source_key
WHERE r.bill_source_key IS NULL