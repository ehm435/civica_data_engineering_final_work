{{ config(materialized='table') }}

with matches as (
    select * from {{ ref('stg_match') }}
),

stages as (
    select * from {{ ref('stg_stage') }}
),

formats as (
    select * from {{ ref('stg_match_format') }}
)

select
    m.match_pk,
    m.match_id,
    m.match_title,
    m.match_date,
    s.stage_name,
    f.format_name,
    m.patch,
    m.event_id

from matches m
left join stages s on m.stage_id = s.stage_pk
left join formats f on m.format_id = f.format_pk