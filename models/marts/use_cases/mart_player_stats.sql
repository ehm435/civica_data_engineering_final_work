
{{ config(
    materialized='table',
    tags=['platinum', 'mart', 'player_stats']
) }}

with gold_facts as (
    select * from {{ ref('fct_player_map_performance') }}
),

dim_player as ( select * from {{ ref('dim_player') }} ),
dim_event  as ( select * from {{ ref('dim_event') }} ),
dim_team   as ( select * from {{ ref('dim_team') }} ),
dim_match  as ( select * from {{ ref('dim_match') }} )

select

    e.event_name,
    m.match_date,
    m.match_title,
    p.player_name,
    t.team_name,
    count(distinct f.map_fk) as maps_played_in_match,
    sum(f.kills) as total_kills,
    sum(f.deaths) as total_deaths,
    round(sum(f.kills) / nullif(sum(f.deaths), 0), 2) as kd_ratio_match,
    round(avg(f.combat_score), 0) as avg_acs_match

from gold_facts f
join dim_player p on f.player_fk = p.player_pk
join dim_team t on f.team_fk = t.team_pk
join dim_event e on f.event_fk = e.event_pk
join dim_match m on f.match_fk = m.match_pk 

group by 1, 2, 3, 4, 5