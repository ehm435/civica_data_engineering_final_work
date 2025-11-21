{{ config(materialized='view') }}

with unioned as (
    select * from {{ ref('base_matches_bangkok_2025') }}
    union all
    select * from {{ ref('base_matches_paris_2025') }}
    union all
    select * from {{ ref('base_matches_toronto_2025') }}
),

with_norm as (
    select
        match_id as match_pk,
        event_id,
        match_title,
        date,
        stage,
        format,
        team1,
        team2,
        score,
        maps_played
    from unioned
)

select
    *
from with_norm