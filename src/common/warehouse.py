import logging
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from google.cloud import bigquery

from src.common.bigquery import (
    delete_unconfigured_observations,
    ensure_ingestion_runs_table,
    ensure_observations_table,
    ensure_series_table,
    load_observations_via_staging,
    load_series_via_staging,
    upsert_ingestion_run,
)

from src.common.logging_config import (
    airflow_context,
    current_parent_run_id,
)

from src.common.timing import (
    Timings,
)

from src.transforms.bcch import (
    prepare_bcch_observations,
)

logger = logging.getLogger(__name__)

# Written when a run completes; the rest is known at start
RUN_COMPLETION_COLUMNS = [
    "status",
    "completed_at",
    "extracted_rows",
    "valid_rows",
    "rejected_rows",
    "loaded_rows",
    "error_message",
    "exit_code",
    "rows_changed",
    "summary_uri",
]

# Written when a run raises before its summary
RUN_CRASH_COLUMNS = [
    "status",
    "completed_at",
    "error_message",
]


def bigquery_enabled() -> bool:

    return (
        os.getenv(
            "BIGQUERY_ENABLED",
            "false",
        ).lower()
        == "true"
    )


def monitoring_dataset() -> str:
    """Dataset of the ingestion_runs audit table (created if missing)."""

    return os.getenv(
        "BIGQUERY_MONITORING_DATASET",
        "monitoring",
    )


def bigquery_target() -> tuple[bigquery.Client, str, str]:
    """Return (client, project_id, dataset_id) from the environment."""

    project_id = os.getenv(
        "GCP_PROJECT_ID"
    )

    dataset_id = os.getenv(
        "BIGQUERY_RAW_DATASET",
        "raw_bcch",
    )

    if not project_id:
        raise RuntimeError(
            "GCP_PROJECT_ID "
            "is not configured."
        )

    client = bigquery.Client(
        project=project_id
    )

    return client, project_id, dataset_id


def publish_to_bigquery(
    df: pd.DataFrame,
    series_config: dict,
    extracted_at: datetime,
    ingestion_run_id: str,
    timings: Timings | None = None,
) -> int:
    """MERGE one series' observations; returns rows changed."""

    if not bigquery_enabled():
        return 0

    client, project_id, dataset_id = (
        bigquery_target()
    )

    ensure_observations_table(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
        runs_dataset_id=monitoring_dataset(),
    )

    warehouse_df = (
        prepare_bcch_observations(
            df=df,
            series_config=series_config,
            extracted_at=extracted_at,
            ingested_at=datetime.now(timezone.utc),
            ingestion_run_id=ingestion_run_id,
        )
    )

    return load_observations_via_staging(
        client=client,
        dataframe=warehouse_df,
        project_id=project_id,
        dataset_id=dataset_id,
        timings=timings,
    )


def prune_unconfigured_observations(
    configured_codes: list[str],
) -> int:
    """Delete raw observations of series no longer in the config."""

    if not bigquery_enabled():
        return 0

    client, project_id, dataset_id = (
        bigquery_target()
    )

    return delete_unconfigured_observations(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
        configured_codes=configured_codes,
    )


def publish_series_to_bigquery(
    warehouse_df: pd.DataFrame,
    configured_codes: list[str],
    timings: Timings | None = None,
) -> int:
    """Load canonical raw_bcch.series rows (see prepare_bcch_series).

    Series not in configured_codes are deleted from the table.
    Returns rows changed.
    """

    if not bigquery_enabled():
        return 0

    if warehouse_df.empty:
        return 0

    client, project_id, dataset_id = (
        bigquery_target()
    )

    ensure_series_table(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
        runs_dataset_id=monitoring_dataset(),
    )

    return load_series_via_staging(
        client=client,
        dataframe=warehouse_df,
        project_id=project_id,
        dataset_id=dataset_id,
        configured_codes=configured_codes,
        timings=timings,
    )


def record_run_started(
    run_id: str,
    pipeline: str,
    table_name: str,
    started_at: datetime,
) -> None:
    """Audit a run in monitoring.ingestion_runs as running.

    Written before any data, so a run killed midway (timeout, OOM) stays
    visible as running. Observability, not data: a failure is logged,
    not fatal.
    """

    airflow = airflow_context() or {}

    _write_run(
        {
            "run_id": run_id,
            "parent_run_id": current_parent_run_id(),
            "source": "bcch",
            "pipeline": pipeline,
            "status": "running",
            "started_at": started_at,
            "airflow_dag_id": airflow.get("dag_id"),
            "airflow_run_id": airflow.get("run_id"),
            "airflow_task_id": airflow.get("task_id"),
            "airflow_try_number": airflow.get("try_number"),
        },
        table_name=table_name,
        update_columns=[],
    )


def record_run_completed(
    summary: dict[str, Any],
    table_name: str,
    exit_code: int,
    summary_uri: str | None,
) -> None:
    """Complete a run's monitoring.ingestion_runs row from its run summary.

    Written for failed runs too (status failed, error_message set). Also
    inserts the full row if the start wasn't recorded.
    """

    airflow = airflow_context() or {}

    _write_run(
        {
            "run_id": summary["run_id"],
            "parent_run_id": summary["parent_run_id"],
            "source": "bcch",
            "pipeline": summary["pipeline"],
            "status": summary["status"],
            "started_at": datetime.fromisoformat(summary["started_at_utc"]),
            "completed_at": datetime.fromisoformat(summary["ended_at_utc"]),
            "exit_code": exit_code,
            "extracted_rows": summary["extracted_rows"],
            "valid_rows": summary["valid_rows"],
            "rejected_rows": summary["rejected_rows"],
            "loaded_rows": summary["loaded_rows"],
            "error_message": summary["error_message"],
            "rows_changed": summary["rows_changed"],
            "summary_uri": summary_uri,
            "airflow_dag_id": airflow.get("dag_id"),
            "airflow_run_id": airflow.get("run_id"),
            "airflow_task_id": airflow.get("task_id"),
            "airflow_try_number": airflow.get("try_number"),
        },
        table_name=table_name,
        update_columns=RUN_COMPLETION_COLUMNS,
    )


def record_run_crashed(
    run_id: str,
    pipeline: str,
    table_name: str,
    started_at: datetime,
    error_message: str,
) -> None:
    """Mark a run that raised before writing its summary as crashed.

    Row counts are unknown: the summary that sums them was never written.
    """

    _write_run(
        {
            "run_id": run_id,
            "parent_run_id": current_parent_run_id(),
            "source": "bcch",
            "pipeline": pipeline,
            "status": "crashed",
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc),
            "error_message": error_message,
        },
        table_name=table_name,
        update_columns=RUN_CRASH_COLUMNS,
    )


def _write_run(
    run: dict[str, Any],
    table_name: str,
    update_columns: list[str],
) -> None:
    """Upsert a run row; table_name: the raw table the run loads."""

    if not bigquery_enabled():
        return

    try:
        client, project_id, dataset_id = bigquery_target()

        run = {**run, "target_table": f"{dataset_id}.{table_name}"}

        ensure_ingestion_runs_table(
            client=client,
            project_id=project_id,
            dataset_id=monitoring_dataset(),
            location_of=dataset_id,
        )

        upsert_ingestion_run(
            client=client,
            project_id=project_id,
            dataset_id=monitoring_dataset(),
            run=run,
            update_columns=update_columns,
        )

    except Exception:
        logger.exception(
            "Could not record run %s (%s) in ingestion_runs",
            run["run_id"],
            run["status"],
        )
