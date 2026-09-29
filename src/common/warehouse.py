import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from src.common.bigquery import (
    ensure_observations_table,
    ensure_series_table,
    load_observations_via_staging,
    load_series_via_staging,
)

from src.transforms.bcch import (
    prepare_bcch_observations,
)


def bigquery_enabled() -> bool:

    return (
        os.getenv(
            "BIGQUERY_ENABLED",
            "false",
        ).lower()
        == "true"
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
) -> int:

    if not bigquery_enabled():
        return 0

    client, project_id, dataset_id = (
        bigquery_target()
    )

    ensure_observations_table(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
    )

    warehouse_df = (
        prepare_bcch_observations(
            df=df,
            series_config=series_config,
            extracted_at=extracted_at,
        )
    )

    return load_observations_via_staging(
        client=client,
        dataframe=warehouse_df,
        project_id=project_id,
        dataset_id=dataset_id,
    )


def publish_series_to_bigquery(
    warehouse_df: pd.DataFrame,
) -> int:
    """Load canonical raw_bcch.series rows (see prepare_bcch_series)."""

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
    )

    return load_series_via_staging(
        client=client,
        dataframe=warehouse_df,
        project_id=project_id,
        dataset_id=dataset_id,
    )
