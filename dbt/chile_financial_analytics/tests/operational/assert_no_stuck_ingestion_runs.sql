-- A run is written as running before it loads anything and completed
-- when it ends. Still running long after the 30-minute Airflow task
-- timeout means the process was killed (timeout, OOM, worker lost):
-- its raw rows may be a partial load. Airflow retries such a task with
-- a new run, hence a warning: check that the retry succeeded.
{{ config(severity='warn') }}

select
    run_id,
    pipeline,
    target_table,
    started_at,
    airflow_run_id,
    airflow_task_id,
    airflow_try_number

from {{ source('monitoring', 'ingestion_runs') }}

where status = 'running'
    and started_at < timestamp_sub(current_timestamp(), interval 2 hour)
