{{ config(
    materialized='table'
) }}

WITH patient_source_data AS (
    SELECT
        patient_id,
        patient_name,
        gender,
        date_of_birth,
        city,
        created_at,
        dbt_valid_from   AS effective_from,
        dbt_valid_to     AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('patient_snapshot') }}
)

SELECT * FROM patient_source_data