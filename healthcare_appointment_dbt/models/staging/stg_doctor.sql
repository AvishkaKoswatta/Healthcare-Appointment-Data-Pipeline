{{ config(materialized='view') }}

with ranked as (
    select
        v:doctor_id::string        as doctor_id,
        v:hospital_id::string      as hospital_id,
        v:doctor_name::string      as doctor_name,
        v:specialty::string        as specialty,
        v:experience_years::integer as experience_years,

         
        to_timestamp(v:created_at::number / 1000000) as created_at,

        current_timestamp as load_timestamp,

        row_number() over (
            partition by v:doctor_id::string
            order by v:created_at::number desc
        ) as rn

    from {{ source('raw', 'doctor') }}
    where v:doctor_id is not null 
     and v:hospital_id is not null
)

select *
from ranked
where rn = 1
