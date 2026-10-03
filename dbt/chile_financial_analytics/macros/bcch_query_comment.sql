{#
    Query comment attached to every BigQuery job dbt runs. With
    `job-label: true` (dbt_project.yml), dbt-bigquery also turns these keys
    into job labels, so INFORMATION_SCHEMA.JOBS can be filtered by dbt
    invocation or Airflow run, the same way as the ingestion jobs
    (src/common/bigquery.py job_labels). Airflow passes AIRFLOW_CTX_* to
    the dbt subprocess; outside Airflow they are empty.
#}
{% macro bcch_query_comment(node) %}

    {%- set comment = {
        "app": "dbt",
        "pipeline": "bcch",
        "dbt_invocation_id": invocation_id,
        "dbt_node": node.unique_id if node is not none else "",
        "airflow_dag_id": env_var("AIRFLOW_CTX_DAG_ID", ""),
        "airflow_run_id": env_var("AIRFLOW_CTX_DAG_RUN_ID", ""),
        "airflow_task_id": env_var("AIRFLOW_CTX_TASK_ID", ""),
        "airflow_try_number": env_var("AIRFLOW_CTX_TRY_NUMBER", ""),
    } -%}

    {{- return(tojson(comment)) -}}

{% endmacro %}
