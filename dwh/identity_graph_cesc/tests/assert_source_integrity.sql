SELECT 1
WHERE (SELECT COUNT(*) FROM {{ source('staging', 'stg_cesc_cih') }}) <> 2191125
   OR (SELECT COUNT(*) FROM {{ source('staging', 'stg_cesc_bill') }}) <> 3730974
