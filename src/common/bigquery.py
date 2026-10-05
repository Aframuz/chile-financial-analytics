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
    "ingestion_run_id",
}

# Safety net: staging tables outlive a killed process (no `finally`).
STAGING_TABLE_TTL = timedelta(hours=1)

# The ingestion run that last wrote the row (raw_bcch.ingestion_runs).
# NULLABLE: added to existing tables (dbt tests it not_null).
INGESTION_RUN_ID_FIELD = bigquery.SchemaField(
    "ingestion_run_id",
    "STRING",
    mode="NULLABLE",
)

# Values for columns added to tables that already had rows, from what
# those rows recorded. extracted_at is the run's start time, which
# make_run_id formatted as the run id (before ids had a random suffix).
LEGACY_BACKFILLS = {
    "ingested_at": "extracted_at",
    "ingestion_run_id": (
        "CONCAT("
        "FORMAT_TIMESTAMP('%Y%m%dT%H%M%S', extracted_at), "
        "FORMAT('%06d', EXTRACT(MICROSECOND FROM extracted_at)), "
        "'Z')"
    ),
}

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
    INGESTION_RUN_ID_FIELD,
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
    INGESTION_RUN_ID_FIELD,
]

SERIES_KEY = [
    "series_code",
]

# monitoring.ingestion_runs: one audit row per ingestion execution,
# written even when it fails. Pipeline history independent of Airflow's
# metadata database; raw rows' ingestion_run_id points here.
INGESTION_RUNS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
    # running → success | failed | crashed; unknown for runs that
    # predate the table (only their id and start time are known)
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    # Fetched from BCCh
    bigquery.SchemaField("extracted_rows", "INT64", mode="NULLABLE"),
    # Passed validation / failed it (validation is per series: a
    # failing series rejects all its rows)
    bigquery.SchemaField("valid_rows", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("rejected_rows", "INT64", mode="NULLABLE"),
    # Written to the warehouse by a committed MERGE
    bigquery.SchemaField("loaded_rows", "INT64", mode="NULLABLE"),
    # Short, redacted; full detail in the run summary (summary_uri)
    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    # Context: where the run fits and where its details are
    bigquery.SchemaField("pipeline", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("target_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("exit_code", "INT64", mode="NULLABLE"),
    # Inserted, updated or deleted by the MERGE (0: nothing new)
    bigquery.SchemaField("rows_changed", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("summary_uri", "STRING", mode="NULLABLE"),
    # The src.pipelines.bcch run the step belonged to, if any
    bigquery.SchemaField("parent_run_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("airflow_dag_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("airflow_run_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("airflow_task_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("airflow_try_number", "STRING", mode="NULLABLE"),
]

INGESTION_RUNS_KEY = [
    "run_id",
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
    runs_dataset_id: str = "monitoring",
) -> bigquery.Table:

    table = ensure_table(
        client=client,
        table_id=(
            f"{project_id}."
            f"{dataset_id}."
            f"observations"
        ),
        schema=OBSERVATIONS_SCHEMA,
        partition_field="observation_date",
        clustering_fields=["series_code"],
    )

    migrate_table(
        client,
        table,
        OBSERVATIONS_SCHEMA,
        pipeline="bcch_ingestion",
        runs_dataset_id=runs_dataset_id,
    )

    return table


def migrate_table(
    client: bigquery.Client,
    table: bigquery.Table,
    schema: list[bigquery.SchemaField],
    pipeline: str,
    runs_dataset_id: str,
) -> None:
    """Bring a raw table created before its current schema up to date.

    Adds missing columns and, in the run that adds them, backfills
    LEGACY_BACKFILLS columns on existing rows. When that gives rows an
    ingestion_run_id, logs those older runs in runs_dataset_id's
    ingestion_runs so every row's run is listed there.
    """

    added = add_missing_columns(client, table, schema)

    backfills = {
        column: LEGACY_BACKFILLS[column]
        for column in added
        if column in LEGACY_BACKFILLS
    }

    if not backfills:
        return

    table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"

    backfill_columns(client, table_id, backfills)

    if "ingestion_run_id" in backfills:
        register_legacy_runs(
            client,
            table_id=table_id,
            target_table=f"{table.dataset_id}.{table.table_id}",
            pipeline=pipeline,
            runs_dataset_id=runs_dataset_id,
        )


def backfill_columns(
    client: bigquery.Client,
    table_id: str,
    expressions: dict[str, str],
) -> int:
    """Fill NULLs in each column with its SQL expression; return rows updated."""

    set_clause = ",\n            ".join(
        f"{column} = COALESCE({column}, {expression})"
        for column, expression in expressions.items()
    )

    where_clause = " OR ".join(
        f"{column} IS NULL"
        for column in expressions
    )

    job = client.query(
        f"""
        UPDATE `{table_id}`
        SET
            {set_clause}
        WHERE {where_clause}
        """,
        job_config=bigquery.QueryJobConfig(
            labels=job_labels(),
        ),
    )

    job.result()

    rows_updated = job.num_dml_affected_rows or 0

    logger.info(
        "Backfilled %s on %s rows of %s (job %s)",
        sorted(expressions),
        rows_updated,
        table_id,
        job.job_id,
    )

    return rows_updated


def register_legacy_runs(
    client: bigquery.Client,
    table_id: str,
    target_table: str,
    pipeline: str,
    runs_dataset_id: str,
) -> int:
    """Log runs that wrote rows before ingestion_runs existed.

    Only the id and start time are known (status unknown); details are
    in the run summary named after the id, in GCS.
    """

    project_id, dataset_id, _ = table_id.split(".")

    ensure_ingestion_runs_table(
        client,
        project_id,
        runs_dataset_id,
        location_of=dataset_id,
    )

    runs_table = f"{project_id}.{runs_dataset_id}.ingestion_runs"

    job = client.query(
        f"""
        INSERT INTO `{runs_table}`
            (run_id, source, pipeline, target_table, status, started_at)
        SELECT
            ingestion_run_id,
            'bcch',
            @pipeline,
            @target_table,
            'unknown',
            MIN(extracted_at)
        FROM `{table_id}`
        WHERE ingestion_run_id NOT IN (
            SELECT run_id FROM `{runs_table}`
        )
        GROUP BY ingestion_run_id
        """,
        job_config=bigquery.QueryJobConfig(
            labels=job_labels(),
            query_parameters=[
                bigquery.ScalarQueryParameter("pipeline", "STRING", pipeline),
                bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
            ],
        ),
    )

    job.result()

    rows_inserted = job.num_dml_affected_rows or 0

    logger.info(
        "Registered %s legacy runs of %s in %s (job %s)",
        rows_inserted,
        target_table,
        runs_table,
        job.job_id,
    )

    return rows_inserted


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
    runs_dataset_id: str = "monitoring",
) -> bigquery.Table:

    # Small dimension-like table:
    # no partitioning or clustering needed
    table = ensure_table(
        client=client,
        table_id=(
            f"{project_id}."
            f"{dataset_id}."
            f"series"
        ),
        schema=SERIES_SCHEMA,
    )

    migrate_table(
        client,
        table,
        SERIES_SCHEMA,
        pipeline="bcch_series_metadata",
        runs_dataset_id=runs_dataset_id,
    )

    return table


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


# ==================================
# monitoring.ingestion_runs
# ==================================

def ensure_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location_of: str,
) -> None:
    """Create dataset_id if missing, in the same location as location_of.

    Same location as the raw data, so queries can join both.
    """

    dataset_ref = f"{project_id}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
        return

    except NotFound:
        pass

    dataset = bigquery.Dataset(dataset_ref)

    dataset.location = client.get_dataset(
        f"{project_id}.{location_of}"
    ).location

    logger.info(
        "Creating dataset %s in %s",
        dataset_ref,
        dataset.location,
    )

    client.create_dataset(dataset, exists_ok=True)


def ensure_ingestion_runs_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location_of: str,
) -> bigquery.Table:
    """Create monitoring.ingestion_runs (and its dataset) if missing.

    location_of: the raw dataset, whose location the dataset takes.
    """

    ensure_dataset(client, project_id, dataset_id, location_of)

    # A few rows per day: no partitioning or clustering needed
    return ensure_table(
        client=client,
        table_id=(
            f"{project_id}."
            f"{dataset_id}."
            f"ingestion_runs"
        ),
        schema=INGESTION_RUNS_SCHEMA,
    )


def build_run_upsert_query(
    table_id: str,
    columns: list[str],
    update_columns: list[str],
) -> str:
    """MERGE one ingestion_runs row from @-parameters, keyed on run_id.

    Inserts the run if it's missing; update_columns are overwritten if
    it exists (none: a rerun of the same statement changes nothing).
    """

    source_columns = ",\n            ".join(
        f"@{column} AS {column}"
        for column in columns
    )

    matched_clause = ""

    if update_columns:

        update_clause = ",\n            ".join(
            f"{column} = source.{column}"
            for column in update_columns
        )

        matched_clause = f"""
    WHEN MATCHED THEN
        UPDATE SET
            {update_clause}
    """

    insert_columns = ", ".join(columns)

    insert_values = ", ".join(
        f"source.{column}"
        for column in columns
    )

    return f"""
    MERGE `{table_id}` AS target

    USING (
        SELECT
            {source_columns}
    ) AS source

    ON target.run_id = source.run_id
    {matched_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """


def upsert_ingestion_run(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    run: dict,
    update_columns: list[str],
) -> None:
    """Write one ingestion_runs row (see build_run_upsert_query)."""

    types = {
        field.name: field.field_type
        for field in INGESTION_RUNS_SCHEMA
    }

    query = build_run_upsert_query(
        table_id=f"{project_id}.{dataset_id}.ingestion_runs",
        columns=list(run),
        update_columns=update_columns,
    )

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            labels=job_labels(),
            query_parameters=[
                bigquery.ScalarQueryParameter(column, types[column], value)
                for column, value in run.items()
            ],
        ),
    )

    job.result()
