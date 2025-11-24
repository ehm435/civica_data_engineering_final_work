{{ config(materialized='view') }}

select
    agent_pk,
    agent_id,
    agent_name
from {{ ref('stg_agent') }}