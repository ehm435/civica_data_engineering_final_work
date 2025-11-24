{{ config(materialized='view') }}

with unioned as (
    {{ union_events(
        table_prefix='base_players', 
        event_suffixes=['bangkok_2025', 'paris_2025', 'toronto_2025'] 
    ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','player_id']) }} as event_player_pk,
    event_id,
    player_id,
    team_id,
    player_name,
    start_date,
    end_date
from unioned
