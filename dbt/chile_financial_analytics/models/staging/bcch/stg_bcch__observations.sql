select
    series_code,
    observation_date,
    value,
    -- Last time the raw row changed (the raw MERGE skips unchanged rows)
    extracted_at
from {{ source('bcch', 'observations') }}
