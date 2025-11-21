{{ config(materialized='view') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['stage']) }} as stage_pk,
    stage as stage_name
from (
    select stage from {{ ref('base_matches_bangkok_2025') }}
    union all
    select stage from {{ ref('base_matches_paris_2025') }}
    union all
    select stage from {{ ref('base_matches_toronto_2025') }}
)
where stage is not null
