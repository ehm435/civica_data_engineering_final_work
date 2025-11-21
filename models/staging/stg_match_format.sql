{{ config(materialized='view') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['format']) }} as format_pk,
    format as format_name
from (
    select format from {{ ref('base_matches_bangkok_2025') }}
    union all
    select format from {{ ref('base_matches_paris_2025') }}
    union all
    select format from {{ ref('base_matches_toronto_2025') }}
)
where format is not null
