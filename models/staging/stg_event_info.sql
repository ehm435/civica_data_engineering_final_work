{{ config(materialized='view') }}

with cte as (
    select
        md5(event_id) as event_id,
        event_id as event_description,
        event_name,
        url,
        title,
        subtitle,
        start_date,
        end_date,
        prize_pool,
        location
    from {{ source('raw_data', 'event_info') }}
)

select
    event_id,
    event_description,
    event_name,
    url,
    title,
    subtitle,
    start_date,
    end_date,
    prize_pool,
    location
from cte
