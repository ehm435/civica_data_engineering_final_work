{{ config(materialized='view') }}

with unioned_stats as (
    select * from {{ ref('base_agents_stats_bangkok_2025') }}
    union all
    select * from {{ ref('base_agents_stats_paris_2025') }}
    union all
    select * from {{ ref('base_agents_stats_toronto_2025') }}
)

select
    agent_id,
    event_id,
    utilization_percent
from unioned_stats
