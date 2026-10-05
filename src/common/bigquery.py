import logging
import re
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

from google.api_core.exceptions import NotFound

import uuid

from src.common.logging_config import (
    airflow_context,
    current_parent_run_id,
    current_run_id,
)
from src.common.timing import Timings

logger = logging.getLogger(__name__)

LABEL_INVALID_CHARS = re.compile(r"[^a-z0-9_-]")

# When a row was extracted or loaded, not what was extracted: excluded
# from change detection so unchanged rows keep the timestamps of their
# last real change.
AUDIT_COLUMNS = {
    "extraction_date",
    "extracted_at",
    "ingested_at",
}

# Safety net: staging tables outlive a killed process (no `finally`).
STAGING_TABLE_TTL = timedelta(hours=1)

OBSERVATIONS_SCHEMA = [
    bigquery.SchemaField(
        "observation_date",
        "DATE",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "series_code",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "series_name",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "value",
        "FLOAT64",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "frequency",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "unit",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "extraction_date",
        "DATE",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "extracted_at",
        "TIMESTAMP",
        mode="REQUIRED",
    ),
    # When the row was last written to the warehouse (dbt source
    # freshness). NULLABLE because it was added to an existing table,
    # and BigQuery only adds NULLABLE columns; dbt tests it not_null.
    bigquery.SchemaField(
        "ingested_at",
        "TIMESTAMP",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "source",
        "STRING",
        mode="REQUIRED",
    ),
]

OBSERVATIONS_KEY = [
    "series_code",
    "observation_date",
]

SERIES_SCHEMA = [
    bigquery.SchemaField(
        "series_code",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "series_name",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "description",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "category",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "frequency",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "unit",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_title_es",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "source_title_en",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "first_observation_date",
        "DATE",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "last_observation_date",
        "DATE",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "source_updated_date",
        "DATE",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "extraction_date",
        "DATE",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "extracted_at",
        "TIMESTAMP",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source",
        "STRING",
        mode="REQUIRED",
    ),
]

SERIES_KEY = [
    "series_code",
]


# ==================================
# GENERIC
# ==================================

def _label_value(value: str) -> str:
    """BigQuery label values: lowercase letters, digits, _ and -; ≤ 63."""

    return LABEL_INVALID_CHARS.sub("_", str(value).lower())[:63]


def job_labels() -> dict[str, str]:
    """Labels linking a BigQuery job to its pipeline run and Airflow task.

    Query them in INFORMATION_SCHEMA.JOBS to find a run's jobs or cost:
        WHERE EXISTS (SELECT 1 FROM UNNEST(labels)
                      WHERE key = 'run_id' AND value = '20261003t…')
    """

    labels = {"pipeline": "bcch"}

    if run_id := current_run_id():
        labels["run_id"] = run_id

    if parent_run_id := current_parent_run_id():
        labels["parent_run_id"] = parent_run_id

    if airflow := airflow_context():
        labels.update(
            {
                "airflow_dag_id": airflow["dag_id"],
                "airflow_run_id": airflow["run_id"],
                "airflow_task_id": airflow["task_id"],
                "airflow_try_number": airflow["try_number"],
            }
        )

    return {
        key: _label_value(value)
        for key, value in labels.items()
    }


def ensure_table(
    client: bigquery.Client,
    table_id: str,
    schema: list[bigquery.SchemaField],
    partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
) -> bigquery.Table:

    try:
        return client.get_table(table_id)

    except NotFound:
        pass

    table = bigquery.Table(
        table_id,
        schema=schema,
    )

    if partition_field:
        table.time_partitioning = (
            bigquery.TimePartitioning(
                type_=(
                    bigquery.TimePartitioningType.DAY
                ),
                field=partition_field,
            )
        )

    if clustering_fields:
        table.clustering_fields = (
            clustering_fields
        )

    logger.info(
        "Creating table %s (partitioned by %s, clustered by %s)",
        table_id,
        partition_field,
        clustering_fields,
    )

    return client.create_table(table)


def add_missing_columns(
    client: bigquery.Client,
    table: bigquery.Table,
    schema: list[bigquery.SchemaField],
) -> list[str]:
    """Append schema columns the table doesn't have yet; return their names.

    Lets a new column in a canonical schema reach tables created before
    it. Only additions: BigQuery adds them as NULLABLE, at the end, and
    existing rows read NULL until backfilled.
    """

    existing = {
        field.name
        for field in table.schema
    }

    missing = [
        field
        for field in schema
        if field.name not in existing
    ]

    if not missing:
        return []

    table.schema = [*table.schema, *missing]

    client.update_table(table, ["schema"])

    added = [field.name for field in missing]

    logger.info(
        "Added columns %s to %s",
        added,
        table.full_table_id,
    )

    return added


def build_merge_query(
    target_table: str,
    staging_table: str,
    schema: list[bigquery.SchemaField],
    key_columns: list[str],
    delete_unmatched: bool = False,
) -> str:
    """Upsert staging rows into the target on key_columns.

    Matched rows are only updated when a non-audit column changed, so
    `extracted_at` records when a row last changed, and downstream
    incremental models can pick up exactly the new and revised rows.

    delete_unmatched: also delete target rows missing from staging,
    except keys in the @retained_keys query parameter (single key only).
    """

    columns = [
        field.name
        for field in schema
    ]

    on_clause = "\n        AND ".join(
        f"target.{column} = source.{column}"
        for column in key_columns
    )

    changed_clause = "\n            OR ".join(
        f"target.{column} IS DISTINCT FROM source.{column}"
        for column in columns
        if column not in key_columns
        and column not in AUDIT_COLUMNS
    )

    update_clause = ",\n            ".join(
        f"{column} = source.{column}"
        for column in columns
        if column not in key_columns
    )

    insert_columns = ",\n            ".join(
        columns
    )

    insert_values = ",\n            ".join(
        f"source.{column}"
        for column in columns
    )

    delete_clause = ""

    if delete_unmatched:

        if len(key_columns) != 1:
            raise ValueError(
                "delete_unmatched requires a single key column."
            )

        delete_clause = f"""
    WHEN NOT MATCHED BY SOURCE
        AND target.{key_columns[0]} NOT IN UNNEST(@retained_keys) THEN
        DELETE
    """

    return f"""
    MERGE `{target_table}` AS target

    USING `{staging_table}` AS source

    ON
        {on_clause}

    WHEN MATCHED AND (
            {changed_clause}
        ) THEN
        UPDATE SET
            {update_clause}

    WHEN NOT MATCHED THEN
        INSERT (
            {insert_columns}
        )
        VALUES (
            {insert_values}
        )
    {delete_clause}"""


def load_via_staging(
    client: bigquery.Client,
    dataframe,
    project_id: str,
    dataset_id: str,
    table_name: str,
    schema: list[bigquery.SchemaField],
    key_columns: list[str],
    retained_keys: list[str] | None = None,
    timings: Timings | None = None,
) -> int:
    """Load into a staging table, then MERGE on key_columns.

    Idempotent: reruns update matched rows
    instead of duplicating them.

    retained_keys: when given, target rows missing from the dataframe
    are deleted unless their key is listed here.

    Returns the rows the MERGE changed (inserted, updated or deleted);
    unchanged rows aren't touched, so a rerun with no news returns 0.

    timings: filled with seconds per BigQuery stage (bq_staging,
    bq_load, bq_merge, bq_cleanup).
    """

    if timings is None:
        timings = Timings()

    labels = job_labels()

    target_table = (
        f"{project_id}."
        f"{dataset_id}."
        f"{table_name}"
    )

    staging_table = (
        f"{project_id}."
        f"{dataset_id}."
        f"_staging_"
        f"{uuid.uuid4().hex}"
    )

    staging = bigquery.Table(
        staging_table,
        schema=schema,
    )

    staging.expires = (
        datetime.now(timezone.utc)
        + STAGING_TABLE_TTL
    )

    staging.labels = labels

    with timings.measure("bq_staging"):
        client.create_table(staging)

    try:

        job_config = (
            bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=(
                    bigquery.WriteDisposition
                    .WRITE_APPEND
                ),
                labels=labels,
            )
        )

        with timings.measure("bq_load"):

            load_job = (
                client.load_table_from_dataframe(
                    dataframe,
                    staging_table,
                    job_config=job_config,
                )
            )

            load_job.result()

        query = build_merge_query(
            target_table=target_table,
            staging_table=staging_table,
            schema=schema,
            key_columns=key_columns,
            delete_unmatched=retained_keys is not None,
        )

        query_config = bigquery.QueryJobConfig(
            query_parameters=(
                [
                    bigquery.ArrayQueryParameter(
                        "retained_keys",
                        "STRING",
                        retained_keys,
                    )
                ]
                if retained_keys is not None
                else []
            ),
            labels=labels,
        )

        with timings.measure("bq_merge"):

            merge_job = client.query(
                query,
                job_config=query_config,
            )

            merge_job.result()

    finally:

        with timings.measure("bq_cleanup"):
            client.delete_table(
                staging_table,
                not_found_ok=True,
            )

    rows_changed = merge_job.num_dml_affected_rows or 0

    # Job ids open the exact jobs in the BigQuery console / INFORMATION_SCHEMA.
    logger.info(
        "MERGE into %s: %s rows sent, %s changed "
        "(load job %s, merge job %s, %s bytes processed; %s)",
        target_table,
        len(dataframe),
        rows_changed,
        load_job.job_id,
        merge_job.job_id,
        merge_job.total_bytes_processed,
        ", ".join(
            f"{stage} {timings[stage]:.2f}s"
            for stage in ("bq_staging", "bq_load", "bq_merge", "bq_cleanup")
        ),
    )

    return rows_changed


# ==================================
# raw_bcch.observations
# ==================================

def ensure_observations_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
) -> bigquery.Table:

    table_id = (
        f"{project_id}."
        f"{dataset_id}."
        f"observations"
    )

    table = ensure_table(
        client=client,
        table_id=table_id,
        schema=OBSERVATIONS_SCHEMA,
        partition_field="observation_date",
        clustering_fields=["series_code"],
    )

    added = add_missing_columns(
        client,
        table,
        OBSERVATIONS_SCHEMA,
    )

    # One-off migration, in the run that adds the column
    if "ingested_at" in added:
        backfill_ingested_at(client, table_id)

    return table


def backfill_ingested_at(
    client: bigquery.Client,
    table_id: str,
) -> int:
    """Set ingested_at on rows loaded before the column existed.

    Their best known load time is extracted_at: the run that wrote them
    loaded within minutes of starting.
    """

    job = client.query(
        f"""
        UPDATE `{table_id}`
        SET ingested_at = extracted_at
        WHERE ingested_at IS NULL
        """,
        job_config=bigquery.QueryJobConfig(
            labels=job_labels(),
        ),
    )

    job.result()

    rows_updated = job.num_dml_affected_rows or 0

    if rows_updated:
        logger.info(
            "Backfilled ingested_at from extracted_at on %s rows of %s "
            "(job %s)",
            rows_updated,
            table_id,
            job.job_id,
        )

    return rows_updated


def load_observations(
    client: bigquery.Client,
    dataframe,
    project_id: str,
    dataset_id: str,
) -> int:

    table_id = (
        f"{project_id}."
        f"{dataset_id}."
        f"observations"
    )

    job_config = (
        bigquery.LoadJobConfig(
            schema=OBSERVATIONS_SCHEMA,
            write_disposition=(
                bigquery.WriteDisposition
                .WRITE_APPEND
            ),
        )
    )

    job = client.load_table_from_dataframe(
        dataframe,
        table_id,
        job_config=job_config,
    )

    job.result()

    return len(dataframe)


def load_observations_via_staging(
    client: bigquery.Client,
    dataframe,
    project_id: str,
    dataset_id: str,
    timings: Timings | None = None,
) -> int:

    return load_via_staging(
        timings=timings,
        client=client,
        dataframe=dataframe,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name="observations",
        schema=OBSERVATIONS_SCHEMA,
        key_columns=OBSERVATIONS_KEY,
    )


def delete_unconfigured_observations(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    configured_codes: list[str],
) -> int:
    """Delete observations of series no longer configured.

    Keeps raw_bcch.observations consistent with raw_bcch.series, which
    drops unconfigured series in its MERGE.
    """

    if not configured_codes:
        raise ValueError(
            "Refusing to prune observations: no configured series."
        )

    query = f"""
    DELETE FROM `{project_id}.{dataset_id}.observations`
    WHERE series_code NOT IN UNNEST(@configured_codes)
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            labels=job_labels(),
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "configured_codes",
                    "STRING",
                    configured_codes,
                )
            ]
        ),
    )

    job.result()

    rows_deleted = job.num_dml_affected_rows or 0

    logger.info(
        "DELETE unconfigured series from %s.%s.observations: "
        "%s rows deleted (job %s)",
        project_id,
        dataset_id,
        rows_deleted,
        job.job_id,
    )

    return rows_deleted


# ==================================
# raw_bcch.series
# ==================================

def ensure_series_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
) -> bigquery.Table:

    # Small dimension-like table:
    # no partitioning or clustering needed
    return ensure_table(
        client=client,
        table_id=(
            f"{project_id}."
            f"{dataset_id}."
            f"series"
        ),
        schema=SERIES_SCHEMA,
    )


def load_series_via_staging(
    client: bigquery.Client,
    dataframe,
    project_id: str,
    dataset_id: str,
    configured_codes: list[str],
    timings: Timings | None = None,
) -> int:
    """MERGE series metadata; delete series no longer configured.

    Configured series missing from the dataframe (e.g. failed
    validation this run) keep their last known metadata.
    """

    return load_via_staging(
        client=client,
        dataframe=dataframe,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name="series",
        schema=SERIES_SCHEMA,
        key_columns=SERIES_KEY,
        retained_keys=configured_codes,
        timings=timings,
    )
