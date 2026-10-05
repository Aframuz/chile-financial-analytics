-- Missing periods between a series' first and last value: a backfill
-- window that failed, or a partial load. A failed run that leaves a hole
-- in the middle of the history isn't caught by the freshness check.
--
--   daily    No more than 7 days between values (business days only;
--            the longest holiday run leaves a 6-day gap).
--   monthly  Every month present.
with observations as (

    select
        observations.series_code,
        series.frequency,
        observations.observation_date,

        lag(observations.observation_date) over (
            partition by observations.series_code
            order by observations.observation_date
        ) as previous_date

    from {{ ref('stg_bcch__observations') }} as observations

    inner join {{ ref('stg_bcch__series') }} as series
        on observations.series_code = series.series_code

    where observations.value is not null

)

select
    series_code,
    frequency,
    previous_date,
    observation_date,
    date_diff(observation_date, previous_date, day) as gap_days

from observations

where case frequency
    when 'daily' then date_diff(observation_date, previous_date, day) > 7
    when 'monthly' then date_diff(observation_date, previous_date, month) > 1
    when 'quarterly' then date_diff(observation_date, previous_date, quarter) > 1
    when 'annual' then date_diff(observation_date, previous_date, year) > 1
end
