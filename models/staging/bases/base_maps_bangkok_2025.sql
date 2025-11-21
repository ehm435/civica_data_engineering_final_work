{{ config(
    materialized = 'view'
) }}

select
    -- identificador del evento para esta tabla
    'masters_bangkok_2025' as event_id, 
    cast(map_id as integer)    as map_id,
    cast(map_name as varchar)  as map_name,
    cast(last_rework as varchar) as last_rework
from {{ source('raw_data', 'maps_bangkok') }}