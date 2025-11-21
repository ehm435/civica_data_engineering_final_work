{{ config(materialized='view') }}

select
    md5('masters_bangkok_2025') as event_id,
    cast(player_id    as integer) as player_id,
    cast(agents_count as integer) as agents_count,
    cast(rounds       as integer) as rounds,
    rating,
    acs,
    kd_ratio,
    kast,
    adr,
    kpr,
    apr,
    fkpr,
    fdpr,
    hs_percent,
    cl_percent,
    cast(clutches     as integer) as clutches,
    cast(k_max        as integer) as k_max,
    cast(kills        as integer) as kills,
    cast(deaths       as integer) as deaths,
    cast(assists      as integer) as assists,
    cast(first_kills  as integer) as first_kills,
    cast(first_deaths as integer) as first_deaths
from {{ source('raw_data', 'player_stats_bangkok') }}
