{{ config(materialized='view') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['player_name']) }} as player_pk,
    player_name
from {{ ref('stg_event_player') }}
where player_name is not null