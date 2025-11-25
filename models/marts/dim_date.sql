{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2026-01-01' as date)"
    ) }}
)

select
    date_day as date_pk,
    year(date_day) as year,
    month(date_day) as month,
    monthname(date_day) as month_name,
    day(date_day) as day,
    quarter(date_day) as quarter,
    dayofweek(date_day) as day_of_week_iso,
    dayname(date_day) as day_name
from date_spine