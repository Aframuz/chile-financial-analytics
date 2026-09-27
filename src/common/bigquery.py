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

    try:
        return client.get_table(table_id)

    except NotFound:
        pass

    table = bigquery.Table(
        table_id,
        schema=OBSERVATIONS_SCHEMA,
    )

    table.time_partitioning = (
        bigquery.TimePartitioning(
            type_=(
                bigquery.TimePartitioningType.DAY
            ),
            field="observation_date",
        )
    )

    table.clustering_fields = [
        "series_code",
    ]

    return client.create_table(table)


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

    staging_table = (
        f"{project_id}."
        f"{dataset_id}."
        f"_staging_"
        f"{uuid.uuid4().hex}"
    )

    job_config = (
        bigquery.LoadJobConfig(
            schema=OBSERVATIONS_SCHEMA,
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

        merge_observations(
            client=client,
            project_id=project_id,
            dataset_id=dataset_id,
            staging_table=staging_table,
        )

    finally:

        client.delete_table(
            staging_table,
            not_found_ok=True,
        )

    return len(dataframe)

def merge_observations(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    staging_table: str,
) -> None:

    target_table = (
        f"{project_id}."
        f"{dataset_id}."
        f"observations"
    )

    query = f"""
    MERGE `{target_table}` AS target

    USING `{staging_table}` AS source

    ON
        target.series_code =
            source.series_code

        AND

        target.observation_date =
            source.observation_date

    WHEN MATCHED THEN
        UPDATE SET

            value =
                source.value,

            series_name =
                source.series_name,

            frequency =
                source.frequency,

            unit =
                source.unit,

            extraction_date =
                source.extraction_date,

            extracted_at =
                source.extracted_at,

            source =
                source.source

    WHEN NOT MATCHED THEN

        INSERT (
            observation_date,
            series_code,
            series_name,
            value,
            frequency,
            unit,
            extraction_date,
            extracted_at,
            source
        )

        VALUES (
            source.observation_date,
            source.series_code,
            source.series_name,
            source.value,
            source.frequency,
            source.unit,
            source.extraction_date,
            source.extracted_at,
            source.source
        )
    """

    query_job = client.query(query)

    query_job.result()