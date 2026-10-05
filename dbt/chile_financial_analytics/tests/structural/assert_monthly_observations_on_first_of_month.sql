-- BCCh dates monthly observations on the first day of the month.
-- Any other day means a mis-parsed date or a series whose frequency
-- changed without its metadata, and would split one month into two
-- rows in reports.
select
    observations.series_code,
    observations.observation_date,
    observations.value

from {{ ref('stg_bcch__observations') }} as observations

inner join {{ ref('stg_bcch__series') }} as series
    on observations.series_code = series.series_code

where series.frequency = 'monthly'
    and extract(day from observations.observation_date) != 1
