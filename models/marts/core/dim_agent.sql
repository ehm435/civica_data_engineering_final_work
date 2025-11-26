{{ config(materialized='table') }}

select
    agent_pk,
    agent_id,
    agent_name
from {{ ref('stg_agent') }}