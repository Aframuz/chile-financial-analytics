with series as (

    select *
    from {{ ref('stg_bcch__series') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['series_code']) }} as series_key,

    series_code,
    series_name,
    series_description,
    category,
    frequency,
    unit,
    source,
    first_observation_date,
    last_observation_date

from series
