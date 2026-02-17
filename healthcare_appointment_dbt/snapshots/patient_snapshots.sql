{% snapshot patient_snapshot %}
{{
    config(
      target_schema='analytic',
      unique_key='patient_id',
      strategy='check',
      check_cols=['patient_name', 'gender', 'date_of_birth', 'city']
    )
}}

SELECT * FROM {{ ref('stg_patient') }}

{% endsnapshot %}
