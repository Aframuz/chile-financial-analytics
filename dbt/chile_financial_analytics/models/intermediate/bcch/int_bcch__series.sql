select distinct
    series_code

from {{ ref('stg_bcch__observations') }}
