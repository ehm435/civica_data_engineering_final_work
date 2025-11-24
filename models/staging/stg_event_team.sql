{{ config(materialized='view') }}

with unioned as (
    {{ union_events(
        table_prefix='base_teams', 
        event_suffixes=['bangkok_2025', 'paris_2025', 'toronto_2025'] 
    ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','team_id']) }} as event_team_pk,
    event_id,
    team_id,
    team_name,
    roster_made
from unioned
