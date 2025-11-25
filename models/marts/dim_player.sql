{{ config(
    materialized='table',
    tags=['gold', 'dimension', 'player']
) }}

with snapshot_data as (

    select 
        player_id,
        player_name,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('players_snapshot') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_pk,
    player_id as player_source_id,
    player_name,
    dbt_valid_from as valid_from

from snapshot_data
where dbt_valid_to is null