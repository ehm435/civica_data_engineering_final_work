{{ config(
    materialized = 'view'
) }}

select
    'masters_bangkok_2025' as event_id, 
    cast(team_id as integer)    as team_id,
    cast(team_name as varchar)  as team_name,
    cast(roster_made as varchar) as roster_made
from {{ source('raw_data', 'teams_bangkok') }}