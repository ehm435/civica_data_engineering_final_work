{{ config(
    materialized='table',
    tags=['platinum', 'mart', 'team_stats']
) }}

select
    e.event_name,
    t.team_name,
    count(distinct f.match_fk) as matches_played,
    sum(f.maps_played) as total_maps_played,
    round(avg(f.is_win) * 100, 2) as map_win_rate_percent,
    sum(f.kills) as team_total_kills,
    sum(f.deaths) as team_total_deaths,
    round(sum(f.kills) / nullif(sum(f.deaths), 0), 2) as team_kd_ratio,
    sum(f.first_kills) as total_first_kills,
    sum(f.first_deaths) as total_first_deaths,
    round(sum(f.first_kills) / nullif(sum(f.first_kills + f.first_deaths), 0) * 100, 2) as first_blood_win_rate

from {{ ref('fct_player_map_performance') }} f
join {{ ref('dim_team') }} t on f.team_fk = t.team_pk
join {{ ref('dim_event') }} e on f.event_fk = e.event_pk
where t.team_name is not null
group by 1, 2