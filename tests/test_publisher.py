from pathlib import Path

from src.common import publisher


class FakeGCSStorage:

    def __init__(
        self,
        bucket_name,
        project_id=None,
    ):
        self.bucket_name = bucket_name


    def upload_file(
        self,
        local_path,
        object_name,
    ):

        return (
            f"gs://{self.bucket_name}/"
            f"{object_name}"
        )
        
def test_gcs_publisher_builds_expected_paths(
    tmp_path,
    monkeypatch,
):

    data_path = (
        tmp_path / "data.json"
    )

    metadata_path = (
        tmp_path / "metadata.json"
    )

    data_path.write_text(
        "{}",
        encoding="utf-8",
    )

    metadata_path.write_text(
        "{}",
        encoding="utf-8",
    )

    monkeypatch.setenv(
        "STORAGE_BACKEND",
        "gcs",
    )

    monkeypatch.setenv(
        "GCS_RAW_BUCKET",
        "test-bucket",
    )

    monkeypatch.setenv(
        "GCP_PROJECT_ID",
        "test-project",
    )

    monkeypatch.setattr(
        publisher,
        "GCSStorage",
        FakeGCSStorage,
    )

    result = (
        publisher.publish_artifacts(
            data_path=data_path,
            metadata_path=metadata_path,
            series_name="usd_clp",
            extraction_date="2026-09-26",
        )
    )

    assert result["data_uri"] == (
        "gs://test-bucket/"
        "raw/bcch/"
        "usd_clp/"
        "extraction_date=2026-09-26/"
        "data.json"
    )