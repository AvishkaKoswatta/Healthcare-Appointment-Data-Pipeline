{{ config(materialized='view') }}

with ranked as (
    select
        v:patient_id::string as patient_id,
        v:patient_name::string as patient_name,
        v:gender::string as gender,

        
        dateadd(
            day,
            v:date_of_birth::number,
            '2000-01-01'::date
        ) as date_of_birth,

        v:city::string as city,

         
        to_timestamp(v:created_at::number / 1000000) as created_at,

        current_timestamp as load_timestamp,

        row_number() over (
            partition by v:patient_id::string
            order by v:created_at::number desc
        ) as rn

    from {{ source('raw', 'patient') }}
    where v:patient_id is not null
)

select *
from ranked
where rn = 1
