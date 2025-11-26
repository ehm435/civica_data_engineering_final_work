{{ config(materialized='table') }}

select
    event_pk,
    event_id,
    event_name,
    location,
    prize_pool,
    start_date,
    end_date
from {{ ref('stg_event') }}