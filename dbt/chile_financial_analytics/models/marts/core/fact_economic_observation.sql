{{
    config(
        materialized='incremental',

        incremental_strategy='merge',

        unique_key=[
            'series_key',
            'observation_date'
        ],

        partition_by={
            'field': 'observation_date',
            'data_type': 'date',
            'granularity': 'day'
        },

        cluster_by=[
            'series_key'
        ]
    )
}}


with observations as (

    select *
    from {{ ref('stg_bcch__observations') }}

    {% if is_incremental() %}

    where observation_date >= date_sub(
        coalesce(
            (
                select max(observation_date)
                from {{ this }}
            ),
            date('1900-01-01')
        ),
        interval 30 day
    )
    
    {% endif %}

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
