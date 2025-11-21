{{ config(materialized='view') }}

select
    cast(agent_id as integer) as agent_id,
    cast(event_id as varchar) as event_id,
    cast(total_utilization as float) as utilization_percent
from {{ source('raw_data', 'agent_stats_table_paris') }}