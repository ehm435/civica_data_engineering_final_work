{{ config(materialized='view') }}

{{ union_events(
    table_prefix='base_player_stats', 
    event_suffixes=['bangkok_2025', 'paris_2025', 'toronto_2025'] 
) }}