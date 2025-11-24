{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with unioned as (
    select
        event_id,
        map_match_id,
        match_id,
        map_id,
        map_order,
        score,
        winner_id  as winner_team_id,
        duration,
        picked_by as picked_by_team_id
    from {{ ref('base_detailed_match_map_bangkok_2025') }}

    union all

    select
        event_id,
        map_match_id,
        match_id,
        map_id,
        map_order,
        score,
        winner_id  as winner_team_id,
        duration,
        picked_by as picked_by_team_id
    from {{ ref('base_detailed_match_map_paris_2025') }}

    union all

    select
        event_id,
        map_match_id,
        match_id,
        map_id,
        map_order,
        score,
        winner_id  as winner_team_id,
        duration,
        picked_by as picked_by_team_id
    from {{ ref('base_detailed_match_map_toronto_2025') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','match_id','map_id','map_order']) }} as match_map_pk,
    event_id,
    map_match_id,
    match_id,
    map_id,
    map_order,
    score,
    winner_team_id,
    duration,
    picked_by_team_id
from unioned
