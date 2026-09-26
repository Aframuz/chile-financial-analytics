import logging
import os

from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any

import bcchapi
import pandas as pd
import yaml

from dotenv import load_dotenv

from src.common.storage import (
    write_dataframe_json_atomic,
    write_json_atomic,
)

from src.validation.bcch import (
    validate_series_data,
)


# PATHS
CONFIG_PATH = Path("config/bcch_series.yml")
RAW_DATA_DIR = Path("data/raw/bcch")
RUNS_DIR = Path("data/_runs/bcch")

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# ==================================
# FUNCTIONS
# ==================================

def create_client() -> bcchapi.Siete:
    """Create a BCCH API client using the `BCCH_API_TOKEN` environment variable."""

    load_dotenv()

    token = os.getenv("BCCH_API_TOKEN")

    if not token:
        raise RuntimeError(
            "BCCH_API_TOKEN environment variable is not configured."
        )

    return bcchapi.Siete(token=token)

def load_config(path: Path) -> dict[str, Any]:
    """Load the configuration from a YAML file."""
    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    if not config:
        raise ValueError("Configuration file is empty.")

    if "series" not in config:
        raise ValueError(
            "Configuration must contain a 'series' section."
        )

    return config


def validate_config(config: dict[str, Any]) -> None:
    """Validate the configuration dictionary by looking for required fields."""
    required_fields = {
        "name",
        "code",
        "start_date",
    }

    seen_names = set()

    for series in config["series"]:
        missing_fields = required_fields - series.keys()

        if missing_fields:
            raise ValueError(
                f"Series configuration is missing fields: "
                f"{missing_fields}"
            )

        series_name = series["name"]

        if series_name in seen_names:
            raise ValueError(
                f"Duplicate series name: {series_name}"
            )

        seen_names.add(series_name)
        
def resolve_end_date(
    configured_end_date: str | None,
) -> str:
    """Resolve the end date for data ingestion.
    Using UTC gives us one consistent technical clock.
    """
    if configured_end_date:
        return configured_end_date

    return datetime.now(timezone.utc).date().isoformat()

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

    status = (
        "success"
        if validation["passed"]
        else "quality_failed"
    )

    if validation["passed"]:

        logger.info(
            "Completed %s: "
            "%s rows written to %s",
            series_name,
            len(df),
            data_path,
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
        "data_path": str(data_path),
        "metadata_path":
            str(metadata_path),
        "checksum_sha256": checksum,
        "quality_passed":
            validation["passed"],
    }
    
def save_run_summary(
    results: list[dict[str, Any]],
    started_at: datetime,
    ended_at: datetime,
    end_date: str,
) -> Path:

    run_id = started_at.strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    success_count = sum(
        result["status"] == "success"
        for result in results
    )

    quality_failed_count = sum(
        result["status"]
        == "quality_failed"
        for result in results
    )

    technical_failed_count = sum(
        result["status"]
        == "technical_failed"
        for result in results
    )

    overall_status = (
        "success"
        if (
            quality_failed_count == 0
            and technical_failed_count == 0
        )
        else "failed"
    )

    summary = {
        "run_id": run_id,
        "pipeline": "bcch_ingestion",

        "status":
            overall_status,

        "started_at_utc":
            started_at.isoformat(),

        "ended_at_utc":
            ended_at.isoformat(),

        "requested_end_date":
            end_date,

        "total_series":
            len(results),

        "success_count":
            success_count,

        "quality_failed_count":
            quality_failed_count,

        "technical_failed_count":
            technical_failed_count,

        "results":
            results,
    }

    run_date = (
        started_at.date().isoformat()
    )

    output_path = (
        RUNS_DIR
        / f"run_date={run_date}"
        / f"{run_id}.json"
    )

    write_json_atomic(
        data=summary,
        path=output_path,
    )

    return output_path
    
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
        end_date=end_date,
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