{{ config(materialized='view') }}

select
    agent_pk,
    agent_name
from {{ ref('stg_agent') }}