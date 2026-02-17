{{ config(
    materialized='table'
) }}

WITH hospital_source_data AS (
    SELECT
        hospital_id,
        hospital_name,
        city,
        country,
        bed_capacity,
        created_at,
        dbt_valid_from   AS effective_from,
        dbt_valid_to     AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('hospital_snapshot') }}
)

SELECT * FROM hospital_source_data