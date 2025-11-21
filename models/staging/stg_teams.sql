{{ config(materialized='view') }}

with unioned as (
    select * from {{ ref('base_teams_bangkok_2025') }}
    union all
    select * from {{ ref('base_teams_paris_2025') }}
    union all
    select * from {{ ref('base_teams_toronto_2025') }}
),

with_norm as (
    select
        event_id,
        team_id,
        team_name,
        lower(trim(team_name)) as team_name_norm,
    from unioned
),

with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key(['event_id', 'team_name_norm']) }} as team_pk,
        md5(event_id) as event_id,
        team_id,
        team_name,
    from with_norm
)

select
    team_pk,
    event_id,
    team_id,
    team_name
from with_pk