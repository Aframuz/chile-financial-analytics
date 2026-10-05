-- An observation can't be dated after the data could have been published.
--
-- Measured against extraction time, not today, so the result doesn't
-- depend on when the test runs:
--
--   daily    BCCh publishes the next business day's value the evening
--            before (the dólar observado for Monday comes out on Friday).
--            Allow 4 days: Friday → Monday, plus a holiday Monday.
--   monthly  A month is published after it closes, so its observation
--            (dated the 1st) must be for a month before the extraction.
--   other    Not after the extraction date.
--
-- extracted_at itself in the future means a clock or timezone bug in
-- ingestion, which would also break incremental loads.
with observations as (

    select
        observations.series_code,
        series.frequency,
        observations.observation_date,
        observations.value,
        observations.extracted_at,
        date(observations.extracted_at, 'America/Santiago') as extracted_date

    from {{ ref('stg_bcch__observations') }} as observations

    inner join {{ ref('stg_bcch__series') }} as series
        on observations.series_code = series.series_code

)

select
    series_code,
    frequency,
    observation_date,
    value,
    extracted_at,

    case
        when extracted_at > current_timestamp() then 'extracted_at in the future'
        else 'observation_date after it could be published'
    end as issue

from observations

where extracted_at > current_timestamp()
    or case frequency
        when 'daily' then observation_date > date_add(extracted_date, interval 4 day)
        when 'monthly' then observation_date >= date_trunc(extracted_date, month)
        else observation_date > extracted_date
    end
