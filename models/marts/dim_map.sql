{{ config(materialized='table') }}

select
    map_pk,
    map_id,
    map_name,
    last_rework
from {{ ref('stg_map') }}