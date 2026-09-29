-- Grain: one observed value for one series on one date.
with observations as (

    select *
    from {{ ref('stg_bcch__observations') }}

),

series as (

    select *
    from {{ ref('dim_series') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

)

select

    series.series_key,

    dates.date_key,

    observations.observation_date,

    observations.value

from observations

left join series
    on observations.series_code = series.series_code

left join dates
    on observations.observation_date = dates.date_day

-- Daily BCCh series include weekends/holidays with no value; those are not observations.
where observations.value is not null
