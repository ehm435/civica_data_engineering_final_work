{{ config(materialized='view') }}

with unioned_stats as (
    select * from {{ ref('base_map_stats_bangkok_2025') }}
    union all
    select * from {{ ref('base_map_stats_paris_2025') }}
    union all
    select * from {{ ref('base_map_stats_toronto_2025') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['event_id', 'map_id']) }} as map_stats_id,
        event_id,
        map_id,
        times_played,
        attack_win_percent,
        defense_win_percent
    from unioned_stats
)

select
    event_id,
    map_id,
    times_played,
    attack_win_percent,
    defense_win_percent
from enriched
