{{ config(materialized='view') }}

with unioned as (
    select
        event_id,
        match_id,
        map_id,
        player_id,
        rating,
        acs,
        k,
        d,
        a,
        kd_diff,
        kast,
        adr,
        hs_percent,
        fk,
        fd,
        fk_fd_diff
    from {{ ref('base_detailed_matches_player_stats_paris_2025') }}

    union all

    select
        event_id,
        match_id,
        map_id,
        player_id,
        rating,
        acs,
        k,
        d,
        a,
        kd_diff,
        kast,
        adr,
        hs_percent,
        fk,
        fd,
        fk_fd_diff
    from {{ ref('base_detailed_matches_player_stats_bangkok_2025') }}

    union all

    select
        event_id,
        match_id,
        map_id,
        player_id,
        rating,
        acs,
        k,
        d,
        a,
        kd_diff,
        kast,
        adr,
        hs_percent,
        fk,
        fd,
        fk_fd_diff
    from {{ ref('base_detailed_matches_player_stats_toronto_2025') }}
),

with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'event_id',
            'match_id',
            'map_id',
            'player_id'
        ]) }} as player_match_map_stats_pk,

        event_id,
        match_id,
        map_id,
        player_id,
        rating,
        acs,
        k,
        d,
        a,
        kd_diff,
        kast,
        adr,
        hs_percent,
        fk,
        fd,
        fk_fd_diff
    from unioned
)

select
    player_match_map_stats_pk,
    event_id,
    match_id,
    map_id,
    player_id,
    rating,
    acs,
    k,
    d,
    a,
    kd_diff,
    kast,
    adr,
    hs_percent,
    fk,
    fd,
    fk_fd_diff
from with_pk
