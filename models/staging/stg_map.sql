{{ config(materialized='view') }}

with unioned as (
    {{ union_events(
        table_prefix='base_maps', 
        event_suffixes=['bangkok_2025', 'paris_2025', 'toronto_2025'] 
    ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['map_id']) }} as map_pk,
    map_id,
    event_id,
    map_name,
    last_rework
from unioned
