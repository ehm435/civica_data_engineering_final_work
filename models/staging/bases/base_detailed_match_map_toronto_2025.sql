{{ config(materialized='view') }}

select
    md5('masters_toronto_2025') as event_id,
    cast(map_match_id as integer) as map_match_id,
    cast(match_id as integer) as match_id,
    cast(map_id as integer) as map_id,
    cast(map_order as integer) as map_order,
    score,
    cast(winner_id as integer) as winner_id,
    cast(duration as integer) as duration,
    cast(picked_by as integer) as picked_by
from {{ source('raw_data', 'detailed_matches_maps_toronto') }}