with series as (

    select *
    from {{ ref('int_bcch__series') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['series_code']) }} as series_key,

    series_code

from series