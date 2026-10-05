-- Warn only: bcch_pipeline has no schedule (manual triggers), so old
-- data isn't a failed run yet. Make it an error once it's scheduled.
{{ config(severity='warn') }}

-- Each series' latest value must be recent for its frequency. Stale data
-- means ingestion stopped (DAG paused, auth expired, series discontinued
-- by BCCh) while every other test keeps passing on old data.
--
--   daily    Within 7 days: the longest holiday run seen (Fiestas
--            Patrias + weekend) leaves a 6-day gap.
--   monthly  Within 3 months: IMACEC and IPC for month M are published
--            in early M+1 / M+2.
with latest as (

    select
        observations.series_code,
        series.series_name,
        series.frequency,
        max(observations.observation_date) as latest_observation_date

    from {{ ref('stg_bcch__observations') }} as observations

    inner join {{ ref('stg_bcch__series') }} as series
        on observations.series_code = series.series_code

    where observations.value is not null

    group by 1, 2, 3

),

thresholds as (

    select
        *,

        case frequency
            when 'daily' then date_sub(current_date('America/Santiago'), interval 7 day)
            when 'monthly' then date_sub(date_trunc(current_date('America/Santiago'), month), interval 3 month)
            when 'quarterly' then date_sub(date_trunc(current_date('America/Santiago'), quarter), interval 6 month)
            when 'annual' then date_sub(date_trunc(current_date('America/Santiago'), year), interval 2 year)
        end as stale_before

    from latest

)

select
    series_code,
    series_name,
    frequency,
    latest_observation_date,
    stale_before

from thresholds

where latest_observation_date < stale_before
