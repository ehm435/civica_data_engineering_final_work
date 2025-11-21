{{ config(materialized='view') }}

with unioned as (
    select event_id, agent_id, utilization_percent
    from {{ ref('base_agents_stats_bangkok_2025') }}

    union all

    select event_id, agent_id, utilization_percent
    from {{ ref('base_agents_stats_paris_2025') }}

    union all

    select event_id, agent_id, utilization_percent
    from {{ ref('base_agents_stats_toronto_2025') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','agent_id']) }} as event_agent_stats_pk,
    event_id,
    agent_id,
    utilization_percent
from unioned
