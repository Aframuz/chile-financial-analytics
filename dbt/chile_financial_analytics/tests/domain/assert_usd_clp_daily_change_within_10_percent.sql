-- USD/CLP moved at most ~5% between consecutive business days since 2020
-- (COVID, 2022 commodity shock). A >10% jump is far more likely a bad
-- value (scaled, shifted decimal, wrong series) than a market move.
with observations as (

    select
        series_code,
        observation_date,
        value,

        lag(observation_date) over (
            partition by series_code
            order by observation_date
        ) as previous_date,

        lag(value) over (
            partition by series_code
            order by observation_date
        ) as previous_value

    from {{ ref('stg_bcch__observations') }}

    where series_code = 'F073.TCO.PRE.Z.D'  -- USD/CLP
        and value is not null

)

select
    series_code,
    previous_date,
    previous_value,
    observation_date,
    value,
    round(safe_divide(value - previous_value, previous_value) * 100, 2) as change_pct

from observations

where abs(safe_divide(value - previous_value, previous_value)) > 0.10
