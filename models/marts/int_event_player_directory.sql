{{ config(materialized='table') }}

with players as (
    select event_id, player_id, player_name, team_id
    from {{ ref('stg_event_player') }}
),

teams as (
    select event_id, team_id, team_name
    from {{ ref('stg_event_team') }}
)

select
    p.event_id,
    p.player_id as global_player_id,
    t.team_name,
    {{ dbt_utils.generate_surrogate_key(['p.player_id']) }} as player_fk,
    {{ dbt_utils.generate_surrogate_key(['t.team_name']) }} as team_fk

from players p
left join teams t 
    on p.event_id = t.event_id 
    and p.team_id = t.team_id