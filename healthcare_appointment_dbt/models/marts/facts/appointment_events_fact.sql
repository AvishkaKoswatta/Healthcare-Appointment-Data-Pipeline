{{ config(
    materialized='incremental'
) }}

select
    e.appointment_id,           
    e.appointment_time,        
    e.consultation_fee,        
    e.event_type,

     
    e.patient_id,
    p.patient_name,
    p.gender,
    p.date_of_birth,

     
    e.doctor_id,
    d.doctor_name,
    d.specialty,
    d.experience_years,

    e.hospital_id,
    h.hospital_name,
    h.city as hospital_city,
    h.country,

     
    case when lower(e.event_type) = 'no_show' then 1 else 0 end as no_show_flag

from {{ ref('stg_appointment_events') }} e
left join {{ ref('patient_snapshot') }} p on e.patient_id = p.patient_id 
left join {{ ref('doctor_snapshot') }} d on e.doctor_id = d.doctor_id 
left join {{ ref('hospital_snapshot') }} h on e.hospital_id = h.hospital_id 