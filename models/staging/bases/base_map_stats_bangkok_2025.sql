{{ config(materialized='view') }}

select
    'masters_bangkok_2025' as event_id,
    cast(map_id as integer) as map_id,
    cast(times_played as integer) as times_played,
    cast(attack_win_percent as float) as attack_win_percent,
    cast(defense_win_percent as float) as defense_win_percent
from {{ source('raw_data', 'map_stats_bangkok') }}