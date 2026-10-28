{{ config(
    materialized='table',
    indexes=[
        {'columns': ['mobile_normalized']}
    ]
) }}

SELECT
    cih_source_row_key,
    mobile_normalized,
    substring(mobile_normalized from 1 for 2) as phone_block_1,
    substring(mobile_normalized from 3 for 2) as phone_block_2,
    substring(mobile_normalized from 5 for 2) as phone_block_3,
    substring(mobile_normalized from 7 for 2) as phone_block_4,
    substring(mobile_normalized from 9 for 2) as phone_block_5
FROM {{ source('staging', 'stg_cesc_cih') }}
WHERE mobile_normalized IS NOT NULL
