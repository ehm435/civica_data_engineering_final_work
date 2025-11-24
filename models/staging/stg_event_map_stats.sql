{{ config(materialized='view') }}

with unioned as (
    {{ union_events(
        table_prefix='base_map_stats', 
        event_suffixes=['bangkok_2025', 'paris_2025', 'toronto_2025'] 
    ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','map_id']) }} as event_map_stats_pk,
    event_id,
    map_id,
    times_played,
    attack_win_percent,
    defense_win_percent
from unioned
