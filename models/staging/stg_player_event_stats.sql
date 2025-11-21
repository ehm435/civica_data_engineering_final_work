{{ config(materialized='view') }}

with unioned as (
    select
        event_id,
        player_id,
        agents_count,
        rounds,
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
        clutches,
        k_max,
        kills,
        deaths,
        assists,
        first_kills,
        first_deaths
    from {{ ref('base_player_stats_bangkok_2025') }}

    union all

    select
        event_id,
        player_id,
        agents_count,
        rounds,
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
        clutches,
        k_max,
        kills,
        deaths,
        assists,
        first_kills,
        first_deaths
    from {{ ref('base_player_stats_paris_2025') }}

    union all

    select
        event_id,
        player_id,
        agents_count,
        rounds,
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
        clutches,
        k_max,
        kills,
        deaths,
        assists,
        first_kills,
        first_deaths
    from {{ ref('base_player_stats_toronto_2025') }}
),

with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'event_id',
            'player_id'
        ]) }} as player_event_stats_pk,

        event_id,
        player_id,
        agents_count,
        rounds,
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
        clutches,
        k_max,
        kills,
        deaths,
        assists,
        first_kills,
        first_deaths
    from unioned
)

select
    player_event_stats_pk,
    event_id,
    player_id,
    agents_count,
    rounds,
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
    clutches,
    k_max,
    kills,
    deaths,
    assists,
    first_kills,
    first_deaths
from with_pk
