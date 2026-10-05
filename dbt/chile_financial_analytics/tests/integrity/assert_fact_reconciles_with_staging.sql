-- The incremental fact must equal a full refresh: every non-null staging
-- observation exactly once, with the same value, and nothing else.
-- Catches a broken incremental filter, a merge that missed a revision,
-- or a delete_stale_observations post-hook that failed.
with expected as (

    select
        series.series_key,
        observations.observation_date,
        observations.value

    from {{ ref('stg_bcch__observations') }} as observations

    inner join {{ ref('dim_series') }} as series
        on observations.series_code = series.series_code

    where observations.value is not null

),

actual as (

    select
        series_key,
        observation_date,
        value

    from {{ ref('fact_economic_observation') }}

)

select
    coalesce(expected.series_key, actual.series_key) as series_key,
    coalesce(expected.observation_date, actual.observation_date) as observation_date,
    expected.value as staging_value,
    actual.value as fact_value,

    case
        when actual.series_key is null then 'missing from fact'
        when expected.series_key is null then 'not in staging'
        else 'value differs'
    end as issue

from expected

full outer join actual
    on expected.series_key = actual.series_key
    and expected.observation_date = actual.observation_date

where expected.series_key is null
    or actual.series_key is null
    or expected.value != actual.value
