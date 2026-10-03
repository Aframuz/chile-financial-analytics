ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION
ARG DBT_CORE_VERSION=1.12.5
ARG DBT_BIGQUERY_VERSION=1.12.1

# Pipeline libraries, pinned alongside the running Airflow version so pip
# can't silently up/downgrade Airflow itself.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt

# dbt lives in its own venv: its dependency pins conflict with Airflow's.
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core==${DBT_CORE_VERSION}" \
      "dbt-bigquery==${DBT_BIGQUERY_VERSION}"
