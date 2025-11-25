{{ config(materialized='table', tags=['platinum', 'player_stats']) }}

select
    e.event_name,
    p.player_name,
    t.team_name,
    count(distinct f.match_fk) as matches_played,
    sum(f.maps_played) as maps_played,
    sum(f.kills) as total_kills,
    sum(f.deaths) as total_deaths,
    round(sum(f.kills) / nullif(sum(f.deaths), 0), 2) as kd_ratio,
    round(avg(f.combat_score), 0) as avg_acs,
    round(avg(f.first_kills), 2) as avg_fk_per_map

from {{ ref('fct_player_map_performance') }} f
left join {{ ref('dim_player') }} p on f.player_fk = p.player_pk
left join {{ ref('dim_team') }} t on f.team_fk = t.team_pk
left join {{ ref('dim_event') }} e on f.event_fk = e.event_pk

group by 1, 2, 3