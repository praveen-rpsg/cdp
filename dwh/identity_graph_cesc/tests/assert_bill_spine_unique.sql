SELECT bill_source_key, COUNT(*)
FROM {{ ref('cesc_bill_spine') }}
GROUP BY bill_source_key
HAVING COUNT(*) > 1
