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
    {{ union_events(
    table_prefix='base_players', 
    event_suffixes=['bangkok_2025', 'paris_2025', 'toronto_2025'] 
) }}
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