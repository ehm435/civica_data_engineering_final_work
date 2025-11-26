{% snapshot players_snapshot %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='player_id',
      strategy='check',
      check_cols=['player_name'],
      invalidate_hard_deletes=True
    )
}}

with all_players as (
    select player_id, player_name, 'bangkok' as source from {{ ref('base_players_bangkok_2025') }}
    union all
    select player_id, player_name, 'paris'   as source from {{ ref('base_players_paris_2025') }}
    union all
    select player_id, player_name, 'toronto' as source from {{ ref('base_players_toronto_2025') }}
),

ranked_players as (
    select 
        player_id, 
        player_name,
        row_number() over (partition by player_id order by source desc) as rn
    from all_players
)

select 
    player_id, 
    player_name 
from ranked_players
where rn = 1

{% endsnapshot %}