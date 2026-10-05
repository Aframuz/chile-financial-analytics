-- BCCh sets the TPM in multiples of 25 basis points. A value like 5.37
-- is not a policy decision: it is an averaged/interpolated series or a
-- different rate loaded under the TPM code.
select
    series_code,
    observation_date,
    value

from {{ ref('stg_bcch__observations') }}

where series_code = 'F022.TPM.TIN.D001.NO.Z.D'  -- TPM
    -- Tolerance for FLOAT64 representation (e.g. 5.2499999999)
    and abs(value * 4 - round(value * 4)) > 1e-9
