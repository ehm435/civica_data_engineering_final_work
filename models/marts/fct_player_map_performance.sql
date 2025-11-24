{{ config(materialized='view') }}

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
    
    s.event_id as event_fk,
    m.match_pk as match_fk,
    {{ dbt_utils.generate_surrogate_key(['s.map_id']) }} as map_fk,
    
    d.player_fk,
    d.team_fk,
    s.rating,
    s.acs,
    s.k as kills,
    s.d as deaths
from stats s
join matches m 
    on s.match_id = m.match_id 
    and s.event_id = m.event_id
left join directory d 
    on s.player_id = d.local_player_id 
    and s.event_id = d.event_id