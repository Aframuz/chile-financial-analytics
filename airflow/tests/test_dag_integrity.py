"""DAG import tests, run against the pinned Airflow runtime.

CI installs the apache-airflow version from airflow/Dockerfile with its
constraints file (see the airflow-dags job in .github/workflows/ci.yml):

    pytest airflow/tests

Parsing is what the scheduler's DAG processor does: these tests catch
import errors, broken top-level code and wiring mistakes before deploy.
They need no database, connections or credentials; anything that does
(e.g. Variable.get) must stay inside tasks, never at module top level.
"""

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

EXPECTED_DAGS = {
    "bcch_financial_pipeline": {
        "ingest_series",
        "ingest_observations",
        "has_changes",
        "snapshot",
        "transform",
        "quality",
        "freshness",
        "report",
    },
    "bcch_backfill": {"ingest_window"},
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_DIR)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_are_loaded(dagbag):
    assert set(dagbag.dag_ids) == set(EXPECTED_DAGS)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_expected_tasks(dagbag, dag_id):
    assert set(dagbag.dags[dag_id].task_ids) == EXPECTED_DAGS[dag_id]


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_one_run_at_a_time(dagbag, dag_id):
    # Concurrent runs would MERGE the same raw tables (see the DAG docs)
    assert dagbag.dags[dag_id].max_active_runs == 1


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_tasks_share_bigquery_pool_and_time_out(dagbag, dag_id):
    for task in dagbag.dags[dag_id].tasks:
        assert task.pool == "bcch_bigquery", task.task_id
        assert task.execution_timeout is not None, task.task_id
