{{ config(materialized='view') }}

with unioned as (
    select event_id, team_id, team_name, roster_made
    from {{ ref('base_teams_bangkok_2025') }}
    union all
    select event_id, team_id, team_name, roster_made
    from {{ ref('base_teams_paris_2025') }}
    union all
    select event_id, team_id, team_name, roster_made
    from {{ ref('base_teams_toronto_2025') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','team_id']) }} as event_team_pk,
    event_id,
    team_id,
    team_name,
    roster_made
from unioned
