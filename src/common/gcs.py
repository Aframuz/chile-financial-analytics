import logging
from pathlib import Path

from google.cloud import storage


logger = logging.getLogger(__name__)


class GCSStorage:
    def __init__(
        self,
        bucket_name: str,
        project_id: str | None = None,
    ) -> None:

        self.client = storage.Client(
            project=project_id
        )

        self.bucket = self.client.bucket(
            bucket_name
        )

        self.bucket_name = bucket_name


    def upload_file(
        self,
        local_path: Path,
        object_name: str,
    ) -> str:

        blob = self.bucket.blob(
            object_name
        )

        blob.upload_from_filename(
            str(local_path),
            checksum="auto",
        )

        uri = (
            f"gs://{self.bucket_name}/"
            f"{object_name}"
        )

        logger.info(
            "Uploaded %s to %s",
            local_path,
            uri,
        )

        return uri