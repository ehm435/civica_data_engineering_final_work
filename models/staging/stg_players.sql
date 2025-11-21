{{ config(materialized='view') }}

with unioned as (
    select * from {{ ref('base_players_bangkok_2025') }}
    union all
    select * from {{ ref('base_players_paris_2025') }}
    union all
    select * from {{ ref('base_players_toronto_2025') }}
),

with_norm as (
    select
        event_id,
        player_id,
        player_name,
        lower(trim(player_name)) as player_name_norm,
        team_id,
        start_date,
        end_date
    from unioned
),

with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key(['event_id', 'player_name_norm']) }} as player_pk,
        event_id,
        player_id,
        player_name,
        team_id,
        start_date,
        end_date
    from with_norm
)

select
        player_pk,
        event_id,
        player_id,
        player_name,
        team_id,
        start_date,
        end_date
from with_pk