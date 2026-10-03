{#
    Post-hook for fact_economic_observation.

    An incremental merge only inserts and updates. Remove fact rows whose
    raw observation is gone: value retracted to null, observation deleted,
    or series no longer configured. Keeps incremental builds equal to a
    full refresh.
#}
{% macro delete_stale_observations() %}

delete from {{ this }} as fact
where not exists (

    select 1

    from {{ ref('stg_bcch__observations') }} as observations

    inner join {{ ref('dim_series') }} as series
        on observations.series_code = series.series_code

    where series.series_key = fact.series_key
        and observations.observation_date = fact.observation_date
        and observations.value is not null

)

{% endmacro %}
