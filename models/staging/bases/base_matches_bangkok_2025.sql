{{ config(
    materialized = 'view'
) }}

select
    match_id,
    cast(event_id as varchar)    as event_id,
    cast(match_title as varchar)  as match_title,
    cast(match_date as date) as date,
    cast(stage as varchar) as stage,
    cast(format as varchar) as format,
    cast(team1_id as varchar) as team1,
    cast(team2_id as varchar) as team2,
    cast(score_overall as varchar) as score,
    cast(maps_played as varchar) as maps_played,
    cast(patch as varchar) as patch
from {{ source('raw_data', 'matches_bangkok') }}