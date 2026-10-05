-- raw_bcch.series holds the enabled series of config/bcch_series.yml.
-- A series with metadata but no facts was configured but never loaded
-- (failed backfill, wrong series code, or start_date after all data).
select
    series.series_key,
    series.series_code,
    series.series_name

from {{ ref('dim_series') }} as series

left join {{ ref('fact_economic_observation') }} as facts
    on series.series_key = facts.series_key

where facts.series_key is null
