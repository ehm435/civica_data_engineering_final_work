{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['agent_id']) }} as agent_pk,
    cast(agent_id as integer) as agent_id,
    agent_name
from {{ source('raw_data', 'agents') }}
