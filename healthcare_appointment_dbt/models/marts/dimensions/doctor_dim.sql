{{ config(
    materialized='table'
) }}

WITH doctor_source_data AS (
    SELECT
        doctor_id,
        doctor_name,
        specialty,
        experience_years,
        created_at,
        dbt_valid_from   AS effective_from,
        dbt_valid_to     AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('doctor_snapshot') }}
)

SELECT * FROM doctor_source_data