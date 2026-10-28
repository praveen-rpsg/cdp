SELECT b.bill_source_key
FROM {{ ref('cesc_bill_spine') }} b
LEFT JOIN {{ ref('cesc_identity_resolution') }} r
    ON b.bill_source_key = r.bill_source_key
WHERE r.bill_source_key IS NULL
