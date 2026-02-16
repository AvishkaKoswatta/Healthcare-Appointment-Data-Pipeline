{{ config(materialized='view') }}

with ranked as (
    select
        v:appointment_id::string    as appointment_id,
        v:patient_id::string        as patient_id,
        v:doctor_id::string         as doctor_id,
        v:hospital_id::string       as hospital_id,
        lower(v:event_type::string) as event_type,

        -- Convert epoch microseconds to timestamp
        to_timestamp(v:appointment_time::number / 1000000)   as appointment_time,
        to_timestamp(v:event_timestamp::number / 1000000)     as event_timestamp,

        v:consultation_fee::numeric as consultation_fee,

        row_number() over (
            partition by v:appointment_id::string
            order by v:event_timestamp::number desc
        ) as rn

    from {{ source('raw', 'appointment_events') }}
    where v:appointment_id is not null
      and v:patient_id is not null

)

select *
from ranked
where rn = 1
