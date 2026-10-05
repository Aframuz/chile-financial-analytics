-- Freshness is checked per series (the observations__* entries in
-- models/staging/bcch/_bcch_sources.yml), each filtered to one
-- series_code. A series added to config/bcch_series.yml without its own
-- entry would silently have no freshness check.
{% set monitored_codes = [] %}

{% if execute %}
    {% for source in graph.sources.values() %}
        {% set filter = (source.freshness or {}).get('filter') or '' %}
        {% if source.identifier == 'observations' and "series_code = '" in filter %}
            {% do monitored_codes.append(filter.split("'")[1]) %}
        {% endif %}
    {% endfor %}
{% endif %}

select
    series_code,
    series_name,
    frequency

from {{ ref('stg_bcch__series') }}

{% if monitored_codes %}
where series_code not in (
    {%- for code in monitored_codes %}
    '{{ code }}'{{ "," if not loop.last }}
    {%- endfor %}
)
{% endif %}
