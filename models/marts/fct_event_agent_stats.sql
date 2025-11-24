{{ config(
    materialized='view' 
) }}

with agent_stats as (
    select event_id, agent_id, utilization_percent 
    from {{ ref('stg_event_agent_stats') }}
),

agent_directory as (
    select agent_id, agent_name 
    from {{ ref('stg_agent') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['as.event_id', 'ad.agent_name']) }} as agent_stats_sk,

    as.event_id as event_fk,
    
    {{ dbt_utils.generate_surrogate_key(['ad.agent_name']) }} as agent_fk,

    as.utilization_percent

from agent_stats as "as"
left join agent_directory ad 
    on as.agent_id = ad.agent_id