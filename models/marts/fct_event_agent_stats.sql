{{ config(
    materialized='view',
    tags=['gold', 'fact', 'meta']
) }}

with stats as (
    select event_id, match_id, map_id, player_id, agent_id, k, d, a, acs
    from {{ ref('stg_player_match_map_stats') }}
),

matches as (
    select event_id, match_id, map_id, winner_team_id
    from {{ ref('stg_match_map') }}
),

player_teams as (
    select event_id, player_id, team_id
    from {{ ref('stg_event_player') }}
),

agent_meta as (
    select agent_id, agent_name
    from {{ ref('stg_agent') }}
),

joined_data as (
    select
        s.event_id,
        s.agent_id,
        s.k, s.d, s.a, s.acs,
        case when pt.team_id = m.winner_team_id then 1 else 0 end as is_win
    from stats s
    join matches m 
        on s.event_id = m.event_id 
        and s.match_id = m.match_id 
        and s.map_id = m.map_id
    join player_teams pt 
        on s.event_id = pt.event_id 
        and s.player_id = pt.player_id
),

aggregated as (
    select
        event_id,
        agent_id,
        count(*) as picks,
        sum(is_win) as wins,
        count(*) - sum(is_win) as losses,
        avg(k) as avg_kills,
        avg(d) as avg_deaths,
        avg(acs) as avg_acs
    from joined_data
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['agg.event_id', 'am.agent_name']) }} as agent_stats_sk,
    agg.event_id as event_fk,
    {{ dbt_utils.generate_surrogate_key(['am.agent_name']) }} as agent_fk,
    agg.picks,
    agg.wins,
    agg.losses,
    round((agg.wins / nullif(agg.picks, 0)) * 100, 2) as win_rate_percent,
    round(agg.avg_kills, 2) as avg_kills_per_map,
    round(agg.avg_deaths, 2) as avg_deaths_per_map,
    round(agg.avg_acs, 2) as avg_acs

from aggregated agg
left join agent_meta am on agg.agent_id = am.agent_id