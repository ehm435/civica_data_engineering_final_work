{{ config(materialized='view') }}

with unioned as (
    {{ union_events(
        table_prefix='base_agents_stats', 
        event_suffixes=['bangkok_2025', 'paris_2025', 'toronto_2025'] 
    ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id','agent_id']) }} as event_agent_stats_pk,
    event_id,
    agent_id,
    utilization_percent
from unioned
