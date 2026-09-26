import os
from pathlib import Path

from src.common.gcs import GCSStorage


def publish_artifacts(
    data_path: Path,
    metadata_path: Path,
    series_name: str,
    extraction_date: str,
) -> dict[str, str]:

    backend = os.getenv(
        "STORAGE_BACKEND",
        "local",
    ).lower()

    if backend == "local":

        return {
            "data_uri": str(data_path),
            "metadata_uri":
                str(metadata_path),
        }

    if backend == "gcs":

        bucket_name = os.getenv(
            "GCS_RAW_BUCKET"
        )

        project_id = os.getenv(
            "GCP_PROJECT_ID"
        )

        if not bucket_name:
            raise RuntimeError(
                "GCS_RAW_BUCKET is not configured."
            )

        storage = GCSStorage(
            bucket_name=bucket_name,
            project_id=project_id,
        )

        prefix = (
            "raw/bcch/"
            f"{series_name}/"
            f"extraction_date="
            f"{extraction_date}"
        )

        data_uri = storage.upload_file(
            local_path=data_path,
            object_name=(
                f"{prefix}/data.json"
            ),
        )

        metadata_uri = storage.upload_file(
            local_path=metadata_path,
            object_name=(
                f"{prefix}/metadata.json"
            ),
        )

        return {
            "data_uri": data_uri,
            "metadata_uri":
                metadata_uri,
        }

    raise ValueError(
        f"Unsupported STORAGE_BACKEND: "
        f"{backend}"
    )