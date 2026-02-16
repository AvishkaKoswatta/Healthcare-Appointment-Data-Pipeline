{% snapshot doctor_snapshot %}
{{
    config(
      target_schema='analytic',
      unique_key='doctor_id',
      strategy='check',
      check_cols=['doctor_name', 'specialty', 'experience_years']
    )
}}

SELECT * FROM {{ ref('stg_doctor') }}

{% endsnapshot %}