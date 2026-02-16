{% snapshot hospital_snapshot %}
{{
    config(
      target_schema='analytic',
      unique_key='hospital_id',
      strategy='check',
      check_cols=['hospital_name', 'city', 'country', 'bed_capacity']
    )
}}

SELECT * FROM {{ ref('stg_hospital') }}

{% endsnapshot %}
