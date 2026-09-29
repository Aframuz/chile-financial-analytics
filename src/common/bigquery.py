from google.cloud import bigquery

from google.api_core.exceptions import NotFound

import uuid

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

    return client.create_table(table)


def build_merge_query(
    target_table: str,
    staging_table: str,
    schema: list[bigquery.SchemaField],
    key_columns: list[str],
) -> str:

    columns = [
        field.name
        for field in schema
    ]

    on_clause = "\n        AND ".join(
        f"target.{column} = source.{column}"
        for column in key_columns
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

    return f"""
    MERGE `{target_table}` AS target

    USING `{staging_table}` AS source

    ON
        {on_clause}

    WHEN MATCHED THEN
        UPDATE SET
            {update_clause}

    WHEN NOT MATCHED THEN
        INSERT (
            {insert_columns}
        )
        VALUES (
            {insert_values}
        )
    """


def load_via_staging(
    client: bigquery.Client,
    dataframe,
    project_id: str,
    dataset_id: str,
    table_name: str,
    schema: list[bigquery.SchemaField],
    key_columns: list[str],
) -> int:
    """Load into a staging table, then MERGE on key_columns.

    Idempotent: reruns update matched rows
    instead of duplicating them.
    """

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

    job_config = (
        bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=(
                bigquery.WriteDisposition
                .WRITE_TRUNCATE
            ),
        )
    )

    load_job = (
        client.load_table_from_dataframe(
            dataframe,
            staging_table,
            job_config=job_config,
        )
    )

    load_job.result()

    try:

        query = build_merge_query(
            target_table=target_table,
            staging_table=staging_table,
            schema=schema,
            key_columns=key_columns,
        )

        client.query(query).result()

    finally:

        client.delete_table(
            staging_table,
            not_found_ok=True,
        )

    return len(dataframe)


# ==================================
# raw_bcch.observations
# ==================================

def ensure_observations_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
) -> bigquery.Table:

    return ensure_table(
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
) -> int:

    return load_via_staging(
        client=client,
        dataframe=dataframe,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name="observations",
        schema=OBSERVATIONS_SCHEMA,
        key_columns=OBSERVATIONS_KEY,
    )


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
) -> int:

    return load_via_staging(
        client=client,
        dataframe=dataframe,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name="series",
        schema=SERIES_SCHEMA,
        key_columns=SERIES_KEY,
    )
