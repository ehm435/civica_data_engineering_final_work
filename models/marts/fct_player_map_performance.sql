{{ config(
    materialized='table',
    tags=['gold', 'fact', 'core']
) }}

with stats as (
    select * from {{ ref('stg_player_match_map_stats') }}
),

matches as (
    select match_pk, event_id, match_id, match_date
    from {{ ref('stg_match') }}
),

map_results as (
    select event_id, match_id, map_id, winner_team_id
    from {{ ref('stg_match_map') }}
),

team_resolver as (
    select event_id, team_id, team_name
    from {{ ref('stg_event_team') }}
),

directory as (
    select * from {{ ref('int_event_player_directory') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.player_match_map_stats_pk']) }} as player_performance_sk,
    {{ dbt_utils.generate_surrogate_key(['s.event_id']) }} as event_fk,
    {{ dbt_utils.generate_surrogate_key(['s.map_id']) }} as map_fk,
    {{ dbt_utils.generate_surrogate_key(['s.player_id']) }} as player_fk,
    {{ dbt_utils.generate_surrogate_key(['s.agent_id']) }} as agent_fk,
    d.team_fk as team_fk,
    m.match_pk as match_fk,
    cast(m.match_date as date) as date_fk,
    s.k as kills,
    s.d as deaths,
    s.a as assists,
    s.kd_diff,
    s.acs as combat_score,
    s.rating,
    s.fk as first_kills,
    s.fd as first_deaths,
    case 
        when d.team_name = win_team.team_name then 1 
        else 0 
    end as is_win,
    
    1 as maps_played

from stats s

join matches m 
    on s.match_id = m.match_id 
    and s.event_id = m.event_id

left join directory d 
    on s.player_id = d.global_player_id 
    and s.event_id = d.event_id
left join map_results mr
    on s.match_id = mr.match_id 
    and s.map_id = mr.map_id 
    and s.event_id = mr.event_id
left join team_resolver win_team
    on mr.winner_team_id = win_team.team_id
    and mr.event_id = win_team.event_id