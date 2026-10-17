# The two runtime pins, read by everything else: scripts/lock-deps.sh,
# CI (DAG import tests, Python version of every job), the Composer deploy
# and terraform/composer.tf, which refuse an environment running another
# Airflow. PYTHON_VERSION is major.minor only: Airflow publishes one
# constraints file per minor version (constraints-3.11.txt).
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

#
# Pipeline libraries, installed with Airflow's tested constraints so pip
# can't silently up/downgrade Airflow or its dependencies. A package in
# both requirements.txt and the constraints must use the constraint's
# version (see requirements.txt).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt lives in its own venv: its dependency pins conflict with Airflow's.
# requirements-dbt.txt is a full lock (scripts/lock-deps.sh), the same one
# CI builds and lints with.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
      -r /tmp/requirements-dbt.txt
