{{ config(materialized='view') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_pk,
    team_name
from {{ ref('stg_event_team') }}
where team_name is not null