-- USD/CLP (dólar observado) is a price: CLP paid for one USD.
-- Zero or negative means a parsing or unit error.
select
    series_code,
    observation_date,
    value

from {{ ref('stg_bcch__observations') }}

where series_code = 'F073.TCO.PRE.Z.D'  -- USD/CLP
    and value <= 0
