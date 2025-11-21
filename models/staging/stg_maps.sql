{{ config(materialized='view') }}

with unioned as (
    select * from {{ ref('base_maps_bangkok_2025') }}
    union all
    select * from {{ ref('base_maps_paris_2025') }}
    union all
    select * from {{ ref('base_maps_toronto_2025') }}
),

with_norm as (
    select
        event_id,
        map_id,
        map_name,
        lower(trim(map_name)) as map_name_norm,
        last_rework
    from unioned
),

with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key(['event_id', 'map_name_norm']) }} as map_pk,
        event_id,
        map_id,
        map_name,
        last_rework
    from with_norm
)

select
    map_pk,
    event_id,
    map_id,
    map_name,
    last_rework
from with_pk