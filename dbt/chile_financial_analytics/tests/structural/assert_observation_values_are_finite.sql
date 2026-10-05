-- FLOAT64 accepts NaN and ±Infinity. NaN is not null, and every
-- comparison with it is false, so it would pass not_null and silently
-- slip through every range check in tests/domain.
select
    series_code,
    observation_date,
    value,
    extracted_at

from {{ ref('stg_bcch__observations') }}

where is_nan(value)
    or is_inf(value)
