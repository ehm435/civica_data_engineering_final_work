{{ config(
    materialized='incremental',
    unique_key=['event_id', 'match_id'],
    incremental_strategy='delete+insert'
) }}

with unioned as (
    select
        event_id,
        match_id,
        match_title,
        cast(date as date) as match_date,
        stage,
        format,
        cast(team1 as integer) as team1_id,
        cast(team2 as integer) as team2_id,
        cast(score as varchar) as score,
        cast(maps_played as integer) as maps_played,
        patch
    from {{ ref('base_matches_bangkok_2025') }}

    union all

    select
        event_id,
        match_id,
        match_title,
        cast(date as date) as match_date,
        stage,
        format,
        cast(team1 as integer) as team1_id,
        cast(team2 as integer) as team2_id,
        cast(score as varchar) as score,
        cast(maps_played as integer) as maps_played,
        patch
    from {{ ref('base_matches_paris_2025') }}

    union all

    select
        event_id,
        match_id,
        match_title,
        cast(date as date) as match_date,
        stage,
        format,
        cast(team1 as integer) as team1_id,
        cast(team2 as integer) as team2_id,
        cast(score as varchar) as score,
        cast(maps_played as integer) as maps_played,
        patch
    from {{ ref('base_matches_toronto_2025') }}
),

with_ids as (
    select
        {{ dbt_utils.generate_surrogate_key(['event_id','match_id']) }} as match_pk,
        event_id,
        match_id,
        match_title,
        match_date,
        {{ dbt_utils.generate_surrogate_key(['stage']) }}  as stage_id,
        {{ dbt_utils.generate_surrogate_key(['format']) }} as format_id,
        team1_id,
        team2_id,
        score,
        maps_played,
        patch
    from unioned
)

select
    match_pk,
    event_id,
    match_id,
    match_title,
    match_date,
    stage_id,
    format_id,
    team1_id,
    team2_id,
    score,
    maps_played,
    patch
from with_ids
