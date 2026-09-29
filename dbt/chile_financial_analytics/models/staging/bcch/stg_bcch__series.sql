select
    series_code,
    series_name,
    description as series_description,
    category,
    frequency,
    unit,
    source,
    source_title_es,
    source_title_en,
    first_observation_date,
    last_observation_date,
    source_updated_date,
    extracted_at
from {{ source('bcch', 'series') }}
