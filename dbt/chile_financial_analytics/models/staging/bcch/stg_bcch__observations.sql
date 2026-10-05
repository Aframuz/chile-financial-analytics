select
    series_code,
    observation_date,
    value,
    -- Last time the raw row changed (the raw MERGE skips unchanged rows)
    extracted_at,
    -- The ingestion run that wrote it (raw_bcch.ingestion_runs)
    ingestion_run_id
from {{ source('bcch', 'observations') }}
