{{ config(materialized='view') }}

with unioned as (
    select * from {{ ref('base_detailed_match_map_bangkok_2025') }}
    union all
    select * from {{ ref('base_detailed_match_map_paris_2025') }}
    union all
    select * from {{ ref('base_detailed_match_map_toronto_2025') }}
),

with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key(['event_id', 'match_id', 'map_id', 'map_order']) }} as map_match_pk,
        event_id,
        match_id,
        map_id,
        map_order,
        score,
        winner_id,
        duration,
        picked_by
    from unioned
)

select * from with_pk
