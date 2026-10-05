-- IMACEC is an activity index (2018 = 100), so it must be positive.
-- Even the 2020 COVID trough stayed above 85; outside [50, 200] the
-- value is almost certainly a different base year or a growth rate
-- (percent) loaded under the index code.
select
    series_code,
    observation_date,
    value

from {{ ref('stg_bcch__observations') }}

where series_code = 'F032.IMC.IND.Z.Z.EP18.Z.Z.0.M'  -- IMACEC
    and (value <= 50 or value >= 200)
