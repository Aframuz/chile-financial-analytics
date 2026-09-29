{% snapshot bcch_series_snapshot %}

{#
    SCD Type 2 history of BCCh series metadata.

    Grain: one row per series_code per version of its tracked attributes.
    The current version has dbt_valid_to = null.
#}

{{
    config(
      schema='snapshots',
      unique_key='series_code',
      strategy='check',
      check_cols=[
          'series_name',
          'frequency',
          'unit',
          'category'
      ],
      hard_deletes='invalidate'
    )
}}

select

    series_code,
    series_name,
    frequency,
    unit,
    category,
    source

from {{ ref('stg_bcch__series') }}

{% endsnapshot %}
