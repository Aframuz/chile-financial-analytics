-- BCCh's catalog says when each series starts. An observation before
-- that date can't be real: it points to rows loaded under the wrong
-- series code. (The end of coverage isn't checked: the catalog can lag
-- the observations it describes.)
select
    observations.series_code,
    observations.observation_date,
    observations.value,
    series.first_observation_date

from {{ ref('stg_bcch__observations') }} as observations

inner join {{ ref('stg_bcch__series') }} as series
    on observations.series_code = series.series_code

where observations.observation_date < series.first_observation_date
