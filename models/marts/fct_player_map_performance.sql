{{ config(
    materialized='view',
    tags=['gold', 'fact', 'performance']
) }}

with stats as (
    select * from {{ ref('stg_player_match_map_stats') }}
),

matches as (
    select match_pk, event_id, match_id, match_date
    from {{ ref('stg_match') }}
),
directory as (
    select * from {{ ref('int_event_player_directory') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.player_match_map_stats_pk']) }} as player_performance_sk,
    {{ dbt_utils.generate_surrogate_key(['s.event_id']) }} as event_fk,
    {{ dbt_utils.generate_surrogate_key(['s.map_id']) }} as map_fk,
    {{ dbt_utils.generate_surrogate_key(['s.player_id']) }} as player_fk,
    d.team_fk as team_fk,
    m.match_pk as match_fk,
    cast(m.match_date as date) as date_fk,
    s.k as kills,
    s.d as deaths,
    s.a as assists,
    s.kd_diff as kill_death_diff,
    s.rating,
    s.acs as average_combat_score,
    s.adr as average_damage_round,
    s.kast as kast_percent,
    s.hs_percent as headshot_percent,
    s.fk as first_kills,
    s.fd as first_deaths,
    s.fk_fd_diff as first_kill_diff

from stats s
join matches m 
    on s.match_id = m.match_id 
    and s.event_id = m.event_id
left join directory d 
    on s.player_id = d.global_player_id 
    and s.event_id = d.event_id