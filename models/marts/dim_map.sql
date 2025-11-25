{{ config(materialized='table') }}

select 
    {{ dbt_utils.generate_surrogate_key(['map_name']) }} as map_pk,
    map_id,
    map_name
from {{ ref('stg_map') }}


QUALIFY row_number() over (partition by map_name order by map_id desc) = 1