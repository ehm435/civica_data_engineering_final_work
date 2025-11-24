{{ config(materialized='view') }}

select
    map_pk,
    map_name,
    last_rework
from {{ ref('stg_map') }}