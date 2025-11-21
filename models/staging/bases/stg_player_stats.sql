{{ config(materialized='view') }}

select * from {{ ref('base_player_stats_paris_2025') }}
union all
select * from {{ ref('base_player_stats_bangkok_2025') }}
union all
select * from {{ ref('base_player_stats_toronto_2025') }}