-- fails if any map has invalid score
{% test map_score_min(model, column_name) %}
with parsed as (
    select
        *,
        regexp_like(score, '^[0-9]+[[:space:]]*-[[:space:]]*[0-9]+$') as valid_format,
        to_number(trim(split_part(score, '-', 1))) as left_score,
        to_number(trim(split_part(score, '-', 2))) as right_score
    from {{ ref('stg_match_map') }}
)

select *
from parsed
where
      valid_format = false
   or left_score is null
   or right_score is null
   or NOT (
            (left_score >= 13 OR right_score >= 13)
        )

{% endtest %}