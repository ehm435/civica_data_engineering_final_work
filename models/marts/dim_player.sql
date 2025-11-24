{{ config(materialized='view') }}

with player_events as (
    select 
        p.player_id,
        p.player_name,
        e.start_date as event_date
    from {{ ref('stg_event_player') }} p
    join {{ ref('stg_event') }} e on p.event_id = e.event_id
),

ranked_players as (
    select
        player_id,
        player_name,
        row_number() over (partition by player_id order by event_date desc) as rn
    from player_events
)

select
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_pk,
    player_id as player_source_id,
    player_name
from ranked_players
where rn = 1