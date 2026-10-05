-- BCCh daily series are published for business days only; the API
-- returns weekends with no value. A weekend value means dates shifted
-- by a timezone conversion (e.g. Monday at 00:00 Santiago read as
-- Sunday in UTC) and every value of the series is off by one day.
select
    observations.series_code,
    observations.observation_date,
    format_date('%A', observations.observation_date) as day_name,
    observations.value

from {{ ref('stg_bcch__observations') }} as observations

inner join {{ ref('stg_bcch__series') }} as series
    on observations.series_code = series.series_code

where series.frequency = 'daily'
    and observations.value is not null
    and extract(dayofweek from observations.observation_date) in (1, 7)
