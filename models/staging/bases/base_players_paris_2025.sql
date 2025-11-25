{{ config(materialized='view') }}

select
    'masters_paris_2025' as event_id,
    cast(player_id as integer) as player_id,
    cast(player_name as varchar) as player_name,
    cast(team_id as integer) as team_id,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date
from {{ source('raw_data', 'players_paris') }}