{{ config(materialized='view') }}

with unioned as (
    select map_id, map_name, last_rework
    from {{ ref('base_maps_bangkok_2025') }}
    union all
    select map_id, map_name, last_rework
    from {{ ref('base_maps_paris_2025') }}
    union all
    select map_id, map_name, last_rework
    from {{ ref('base_maps_toronto_2025') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['map_id']) }} as map_pk,
    map_id,
    map_name,
    last_rework
from unioned
