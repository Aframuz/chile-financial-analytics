-- Monthly CPI variation, in percent: -0.5% to 1.9% since 2020, through
-- the 2022 inflation peak. Outside [-3, 5] the value is more likely the
-- CPI index level or a 12-month variation than a monthly one.
select
    series_code,
    observation_date,
    value

from {{ ref('stg_bcch__observations') }}

where series_code = 'G073.IPC.VAR.2023.M'  -- IPC Monthly Variation
    and (value < -3 or value > 5)
