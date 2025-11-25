{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as event_pk,
    event_id,
    event_id as event_code,
    event_name,
    url,
    title,
    subtitle,
    start_date,
    end_date,
    prize_pool,
    location
from {{ source('raw_data', 'event_info') }}
