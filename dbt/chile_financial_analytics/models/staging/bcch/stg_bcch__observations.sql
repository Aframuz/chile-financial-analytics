select
    series_code,
    observation_date,
    value
from {{ source('bcch', 'observations') }}
