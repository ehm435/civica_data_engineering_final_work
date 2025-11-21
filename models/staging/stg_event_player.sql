{{ config(materialized='view') }}

with unioned as (
    select event_id, player_id, player_name, team_id, start_date, end_date
    from {{ ref('base_players_bangkok_2025') }}
    union all
    select event_id, player_id, player_name, team_id, start_date, end_date
    from {{ ref('base_players_paris_2025') }}
    union all
    select event_id, player_id, player_name, team_id, start_date, end_date
    from {{ ref('base_players_toronto_2025') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','player_id']) }} as event_player_pk,
    event_id,
    player_id,
    player_name,
    team_id,
    start_date,
    end_date
from unioned
