with observations as (

    select *
    from {{ ref('stg_bcch__observations') }}

),

series as (

    select *
    from {{ ref('dim_series') }}

)

select

    series.series_key,

    observations.observation_date,

    observations.value

from observations

left join series
    on observations.series_code = series.series_code

-- Daily BCCh series include weekends/holidays with no value; those are not observations.
where observations.value is not null
