{{ config(materialized='view') }}

with unioned as (
    select event_id, map_id, times_played, attack_win_percent, defense_win_percent
    from {{ ref('base_map_stats_bangkok_2025') }}

    union all

    select event_id, map_id, times_played, attack_win_percent, defense_win_percent
    from {{ ref('base_map_stats_paris_2025') }}

    union all

    select event_id, map_id, times_played, attack_win_percent, defense_win_percent
    from {{ ref('base_map_stats_toronto_2025') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','map_id']) }} as event_map_stats_pk,
    event_id,
    map_id,
    times_played,
    attack_win_percent,
    defense_win_percent
from unioned
