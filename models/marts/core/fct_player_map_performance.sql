{{ config(
    materialized='incremental',
    unique_key='player_performance_sk',
    incremental_strategy='merge',
    tags=['gold', 'fact', 'performance']
) }}

with stats as (
    select * from {{ ref('stg_player_match_map_stats') }}
),

matches as (
    select match_pk, event_id, match_id, match_date
    from {{ ref('stg_match') }}
    {% if is_incremental() %}
    where match_date >= (select coalesce(max(date_fk), '1900-01-01') from {{ this }})
    {% endif %}
),

map_results as (
    select event_id, match_id, map_id, winner_team_id
    from {{ ref('stg_match_map') }}
),

map_name_resolver as (
    select event_id, map_id, map_name
    from {{ ref('stg_map') }}
),

team_resolver as (
    select event_id, team_id, team_name 
    from {{ ref('stg_event_team') }}
),

player_roster as (
    select p.event_id, p.player_id, t.team_name
    from {{ ref('stg_event_player') }} p
    join {{ ref('stg_event_team') }} t 
        on p.event_id = t.event_id 
        and p.team_id = t.team_id
)

select
    {{ dbt_utils.generate_surrogate_key(['s.player_match_map_stats_pk']) }} as player_performance_sk,
    {{ dbt_utils.generate_surrogate_key(['s.event_id']) }} as event_fk,
    {{ dbt_utils.generate_surrogate_key(['mnr.map_name']) }} as map_fk,
    {{ dbt_utils.generate_surrogate_key(['s.player_id']) }} as player_fk,
    {{ dbt_utils.generate_surrogate_key(['s.agent_id']) }} as agent_fk,
    {{ dbt_utils.generate_surrogate_key(['pr.team_name']) }} as team_fk,
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
        when pr.team_name = win_team.team_name then 1 
        else 0 
    end as is_win,
    
    1 as maps_played

from stats s

join matches m 
    on s.match_id = m.match_id 
    and s.event_id = m.event_id

left join player_roster pr
    on s.player_id = pr.player_id 
    and s.event_id = pr.event_id

left join map_results mr
    on s.match_id = mr.match_id 
    and s.map_id = mr.map_id 
    and s.event_id = mr.event_id

left join map_name_resolver mnr
    on s.map_id = mnr.map_id
    and s.event_id = mnr.event_id
    
left join team_resolver win_team
    on mr.winner_team_id = win_team.team_id
    and mr.event_id = win_team.event_id

{% if is_incremental() %}
QUALIFY row_number() over (partition by player_performance_sk order by date_fk desc) = 1
{% endif %}