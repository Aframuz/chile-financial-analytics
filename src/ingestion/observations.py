"""BCCh observations pipeline.

BCCh → validate → data/raw/bcch/{series_name}/ → GCS → raw_bcch.observations
"""

import logging

from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any

import bcchapi
import pandas as pd

from src.common.storage import (
    write_dataframe_json_atomic,
    write_json_atomic,
)

from src.validation.bcch import (
    validate_series_data,
)

from src.common.publisher import (
    publish_artifacts,
)

from src.common.warehouse import (
    publish_to_bigquery,
)

from src.ingestion.bcch import (
    CONFIG_PATH,
    RAW_DATA_DIR,
    RUNS_DIR,
    create_client,
    load_config,
    resolve_end_date,
    save_run_summary,
    validate_config,
)


# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# ==================================
# FUNCTIONS
# ==================================

def extract_series(
    client: bcchapi.Siete,
    series_code: str,
    series_name: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:

    """Extract a series from the BCCH API."""

    # log the extraction process
    logger.info(
        "Extracting %s (%s) from %s to %s",
        series_name,
        series_code,
        start_date,
        end_date,
    )

    # Extract the series data from the BCCH API
    df = client.cuadro(
        series=[series_code],
        nombres=[series_name],
        desde=start_date,
        hasta=end_date,
    )

    return df

def build_output_dir(
    series_name: str,
    extraction_date: str,
) -> Path:
    """Build the output directory path for the extracted data."""
    return (
        RAW_DATA_DIR
        / series_name
        / f"extraction_date={extraction_date}"
    )

def save_raw(
    df: pd.DataFrame,
    output_dir: Path,
) -> tuple[Path, str]:
    """Save the raw DataFrame to a JSON file in the specified output directory."""
    output_path = (
        output_dir / "data.json"
    )

    checksum = (
        write_dataframe_json_atomic(
            df=df,
            path=output_path,
        )
    )

    return output_path, checksum

def save_metadata(
    df: pd.DataFrame,
    series_config: dict[str, Any],
    start_date: str,
    end_date: str,
    extracted_at: datetime,
    output_dir: Path,
    checksum: str,
    validation: dict[str, Any],
) -> Path:
    """Save metadata about the extracted data to a JSON file in the specified output directory."""
    metadata = {
        "source":
            "Banco Central de Chile - BDE",

        "series_code":
            series_config["code"],

        "series_name":
            series_config["name"],

        "frequency":
            series_config.get(
                "frequency"
            ),

        "unit":
            series_config.get(
                "unit"
            ),

        "requested_start_date":
            start_date,

        "requested_end_date":
            end_date,

        "first_observation": (
            str(df.index.min())
            if not df.empty
            else None
        ),

        "last_observation": (
            str(df.index.max())
            if not df.empty
            else None
        ),

        "row_count":
            len(df),

        "extracted_at_utc":
            extracted_at.isoformat(),

        "extractor":
            "bcchapi",

        "bcchapi_version":
            version("bcchapi"),

        "data_file_sha256":
            checksum,

        "quality": validation,
    }

    metadata_path = (
        output_dir
        / "metadata.json"
    )

    write_json_atomic(
        data=metadata,
        path=metadata_path,
    )

    return metadata_path

def ingest_series(
    client: bcchapi.Siete,
    series_config: dict[str, Any],
    end_date: str,
    run_started_at: datetime,
) -> dict[str, Any]:
    """Ingest a single series based on the provided configuration."""
    series_name = (
        series_config["name"]
    )

    series_code = (
        series_config["code"]
    )

    start_date = (
        series_config["start_date"]
    )

    logger.info(
        "Starting ingestion for %s",
        series_name,
    )

    df = extract_series(
        client=client,
        series_code=series_code,
        series_name=series_name,
        start_date=start_date,
        end_date=end_date,
    )

    validation = validate_series_data(
        df=df,
        series_config=series_config,
        start_date=start_date,
        end_date=end_date,
    )

    extraction_date = (
        run_started_at
        .date()
        .isoformat()
    )

    output_dir = build_output_dir(
        series_name=series_name,
        extraction_date=extraction_date,
    )

    data_path, checksum = save_raw(
        df=df,
        output_dir=output_dir,
    )

    metadata_path = save_metadata(
        df=df,
        series_config=series_config,
        start_date=start_date,
        end_date=end_date,
        extracted_at=run_started_at,
        output_dir=output_dir,
        checksum=checksum,
        validation=validation,
    )

    published = publish_artifacts(
        data_path=data_path,
        metadata_path=metadata_path,
        series_name=series_name,
        extraction_date=extraction_date,
    )


    status = (
        "success"
        if validation["passed"]
        else "quality_failed"
    )

    warehouse_rows = 0

    if validation["passed"]:

        logger.info(
            "Completed %s: "
            "%s rows written to %s",
            series_name,
            len(df),
            data_path,
        )

        warehouse_rows = publish_to_bigquery(
            df=df,
            series_config=series_config,
            extracted_at=run_started_at,
        )

    else:

        logger.error(
            "Data-quality validation "
            "failed for %s",
            series_name,
        )

    return {
        "series_name": series_name,
        "series_code": series_code,
        "status": status,
        "row_count": len(df),
        "local_data_path": str(data_path),
        "local_metadata_path": str(metadata_path),
        "data_uri": published["data_uri"],
        "metadata_uri": published["metadata_uri"],
        "checksum_sha256": checksum,
        "quality_passed":validation["passed"],
        "warehouse_rows": warehouse_rows,
    }

# =======================
# MAIN
# =======================
def main() -> int:
    results: list[dict[str, Any]] = []

    config = load_config(CONFIG_PATH)

    validate_config(config)

    client = create_client()

    run_started_at = datetime.now(
        timezone.utc
    )

    configured_end_date = (
        config
        .get("defaults", {})
        .get("end_date")
    )

    end_date = resolve_end_date(
        configured_end_date
    )

    for series_config in config["series"]:

        if not series_config.get(
            "enabled",
            True,
        ):

            logger.info(
                "Skipping disabled series: %s",
                series_config["name"],
            )

            continue

        try:

            result = ingest_series(
                client=client,
                series_config=series_config,
                end_date=end_date,
                run_started_at=run_started_at,
            )

            results.append(result)

        except Exception as exc:

            logger.exception(
                "Technical failure while "
                "ingesting %s",
                series_config["name"],
            )

            results.append(
                {
                    "series_name":
                        series_config["name"],

                    "series_code":
                        series_config["code"],

                    "status":
                        "technical_failed",

                    "error":
                        str(exc),
                }
            )

    ended_at = datetime.now(
    timezone.utc
    )

    summary_path = save_run_summary(
        results=results,
        started_at=run_started_at,
        ended_at=ended_at,
        pipeline="bcch_ingestion",
        runs_dir=RUNS_DIR,
        details={
            "requested_end_date": end_date,
        },
    )

    has_failures = any(
        result["status"] != "success"
        for result in results
    )

    logger.info(
        "Run summary written to %s",
        summary_path,
    )

    if has_failures:
        return 1

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
