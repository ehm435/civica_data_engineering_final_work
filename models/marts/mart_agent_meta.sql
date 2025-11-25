{{ config(materialized='table', tags=['platinum', 'agent_meta']) }}

select
    e.event_name,
    a.agent_name,
    sum(f.maps_played) as total_picks,
    sum(f.is_win) as total_wins,
    sum(f.maps_played) - sum(f.is_win) as total_losses,
    round(sum(f.is_win) / nullif(sum(f.maps_played), 0) * 100, 2) as win_rate_percent,
    round(avg(f.kills), 2) as avg_kills,
    round(avg(f.deaths), 2) as avg_deaths,
    round(avg(f.combat_score), 0) as avg_acs

from {{ ref('fct_player_map_performance') }} f
join {{ ref('dim_agent') }} a on f.agent_fk = a.agent_pk
join {{ ref('dim_event') }} e on f.event_fk = e.event_pk

group by 1, 2