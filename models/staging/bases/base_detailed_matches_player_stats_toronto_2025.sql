{{ config(materialized='view') }}

select
    md5('masters_toronto_2025') as event_id,
    cast(match_id  as integer) as match_id,
    cast(player_id as integer) as player_id,
    cast(map_id    as integer) as map_id,
    rating,
    acs,
    cast(k        as integer) as k,
    cast(d        as integer) as d,
    cast(a        as integer) as a,
    cast(kd_diff  as integer) as kd_diff,
    kast,
    adr,
    hs_percent,
    cast(fk       as integer) as fk,
    cast(fd       as integer) as fd,
    cast(fk_fd_diff as integer) as fk_fd_diff
from {{ source('raw_data', 'detailed_matches_player_stats_toronto') }}