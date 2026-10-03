import os
from pathlib import Path

from src.common.gcs import GCSStorage


def publish_artifacts(
    data_path: Path,
    metadata_path: Path,
    series_name: str,
    extraction_date: str,
    window: str | None = None,
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

        # Mirrors build_output_dir: one folder per explicit window
        if window:
            prefix += f"/window={window}"

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

def publish_run_summary(summary_path: Path) -> str:
    """Publish a run summary; returns its URI (local path or gs://).

    data/_runs/... is mirrored as gs://<bucket>/_runs/..., so summaries
    survive ephemeral workers and sit next to the raw data they describe.
    """

    backend = os.getenv(
        "STORAGE_BACKEND",
        "local",
    ).lower()

    if backend == "local":
        return str(summary_path)

    if backend == "gcs":

        bucket_name = os.getenv(
            "GCS_RAW_BUCKET"
        )

        if not bucket_name:
            raise RuntimeError(
                "GCS_RAW_BUCKET is not configured."
            )

        storage = GCSStorage(
            bucket_name=bucket_name,
            project_id=os.getenv("GCP_PROJECT_ID"),
        )

        parts = summary_path.parts

        object_name = "/".join(
            parts[1:] if parts and parts[0] == "data" else parts
        )

        return storage.upload_file(
            local_path=summary_path,
            object_name=object_name,
        )

    raise ValueError(
        f"Unsupported STORAGE_BACKEND: "
        f"{backend}"
    )
