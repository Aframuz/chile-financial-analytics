-- SCD Type 2: at any point in time a series has at most one valid
-- version, so an as-of join to the snapshot never fans out. Gaps are
-- allowed: hard_deletes='invalidate' closes a removed series, and
-- re-adding it later opens a new version.
with versions as (

    select
        series_code,
        dbt_valid_from,
        dbt_valid_to,

        lead(dbt_valid_from) over (
            partition by series_code
            order by dbt_valid_from
        ) as next_valid_from

    from {{ ref('bcch_series_snapshot') }}

)

select
    series_code,
    dbt_valid_from,
    dbt_valid_to,
    next_valid_from,

    case
        when dbt_valid_to <= dbt_valid_from then 'ends before it starts'
        when dbt_valid_to is null then 'superseded version still current'
        else 'overlaps next version'
    end as issue

from versions

where dbt_valid_to <= dbt_valid_from
    or (next_valid_from is not null and dbt_valid_to is null)
    or dbt_valid_to > next_valid_from
