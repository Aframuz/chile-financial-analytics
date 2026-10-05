{#
    on_schema_change: new columns reach the existing table on the next
    incremental run, NULL for older rows until a one-off --full-refresh.
#}
{{
    config(
        materialized='incremental',

        incremental_strategy='merge',

        unique_key=[
            'series_key',
            'observation_date'
        ],

        partition_by={
            'field': 'observation_date',
            'data_type': 'date',
            'granularity': 'day'
        },

        cluster_by=[
            'series_key'
        ],

        on_schema_change='append_new_columns',

        post_hook=[
            "{{ delete_stale_observations() }}"
        ]
    )
}}


with observations as (

    select *
    from {{ ref('stg_bcch__observations') }}

    {% if is_incremental() %}

    -- New and revised raw rows since the last build, at any observation
    -- date: late-arriving monthly values and old revisions included.
    where extracted_at > (
        select coalesce(
            max(extracted_at),
            timestamp('1900-01-01')
        )
        from {{ this }}
    )

    {% endif %}

),

series as (

    select *
    from {{ ref('dim_series') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

)

select

    series.series_key,

    dates.date_key,

    observations.observation_date,

    observations.value,

    observations.extracted_at,

    -- Lineage: the ingestion run that wrote the raw row (raw_bcch.ingestion_runs)
    observations.ingestion_run_id

from observations

left join series
    on observations.series_code = series.series_code

left join dates
    on observations.observation_date = dates.date_day

-- Daily BCCh series include weekends/holidays with no value; those are not observations.
where observations.value is not null
