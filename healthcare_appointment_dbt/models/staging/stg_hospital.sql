{{ config(materialized='view') }}

with ranked as (
    select
        v:hospital_id::string as hospital_id,
        v:hospital_name::string as hospital_name,
        v:city::string as city,
        v:country::string as country,
        v:bed_capacity::integer as bed_capacity,

        -- Convert postgres logical timestamp (microseconds)
        to_timestamp(v:created_at::number / 1000000) as created_at,

        current_timestamp as load_timestamp,

        row_number() over (
            partition by v:hospital_id::string
            order by v:created_at::number desc
        ) as rn

    from {{ source('raw', 'hospital') }}
        where v:hospital_id is not null
        
)

select *
from ranked
where rn = 1
