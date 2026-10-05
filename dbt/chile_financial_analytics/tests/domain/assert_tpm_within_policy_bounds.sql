-- TPM (monetary policy rate) is an annual percentage: 0.5% to 11.25%
-- since 2020. A negative rate or one above 25% is a unit error, e.g.
-- basis points (525) instead of percent (5.25). A fraction (0.0525) is
-- caught by assert_tpm_moves_in_quarter_point_steps.
select
    series_code,
    observation_date,
    value

from {{ ref('stg_bcch__observations') }}

where series_code = 'F022.TPM.TIN.D001.NO.Z.D'  -- TPM
    and (value < 0 or value > 25)
