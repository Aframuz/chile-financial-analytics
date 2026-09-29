-- Grain: one row per calendar day, contiguous, from the earliest
-- to the latest observation date across all BCCh series.
with bounds as (

    select
        min(observation_date) as min_date,
        max(observation_date) as max_date

    from {{ ref('stg_bcch__observations') }}

),

dates as (

    select date_day

    from bounds,
    unnest(
        generate_date_array(
            min_date,
            max_date
        )
    ) as date_day

)

select

    -- Surrogate key, conventional YYYYMMDD integer (e.g. 20260925)
    cast(format_date('%Y%m%d', date_day) as int64) as date_key,

    date_day,

    extract(year from date_day) as year,

    extract(quarter from date_day) as quarter,

    extract(month from date_day) as month,

    extract(day from date_day) as day,

    -- BigQuery convention: Sunday = 1 ... Saturday = 7
    extract(dayofweek from date_day) as day_of_week,

    format_date('%B', date_day) as month_name,

    format_date('%A', date_day) as day_name,

    extract(dayofweek from date_day) in (1, 7) as is_weekend

from dates
