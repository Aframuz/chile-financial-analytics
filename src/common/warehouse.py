import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from src.common.bigquery import (
    ensure_observations_table,
    load_observations_via_staging,
)

from src.transforms.bcch import (
    prepare_bcch_observations,
)

def publish_to_bigquery(
    df: pd.DataFrame,
    series_config: dict,
    extracted_at: datetime,
) -> int:

    enabled = (
        os.getenv(
            "BIGQUERY_ENABLED",
            "false",
        ).lower()
        == "true"
    )

    if not enabled:
        return 0

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