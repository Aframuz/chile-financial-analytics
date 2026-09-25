import json
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

# PATHS
CONFIG_PATH = Path("config/bcch_series.yml")
RAW_DATA_DIR = Path("data/raw/bcch")

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

    # Check if the DataFrame is empty and raise an error if so
    if df.empty:
        raise RuntimeError(
            f"No data returned for {series_name} "
            f"({series_code})."
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
) -> Path:
    """Save the raw data to a JSON file in the specified output directory.
    If the directory already exists, that's okay."""
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = output_dir / "data.json"

    df.to_json(
        output_path,
        orient="table",
        date_format="iso",
        indent=2,
    )

    return output_path

def save_metadata(
    df: pd.DataFrame,
    series_config: dict[str, Any],
    start_date: str,
    end_date: str,
    extracted_at: datetime,
    output_dir: Path,
) -> Path:
    """Save metadata about the extracted data to a JSON file in the specified output directory."""
    
    # Build the metadata dictionary
    metadata = {
        "source": "Banco Central de Chile - BDE",
        "series_code": series_config["code"],
        "series_name": series_config["name"],
        "frequency": series_config.get("frequency"),
        "unit": series_config.get("unit"),
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "first_observation": str(df.index.min()),
        "last_observation": str(df.index.max()),
        "row_count": len(df),
        "extracted_at_utc": extracted_at.isoformat(),
        "extractor": "bcchapi",
        "bcchapi_version": version("bcchapi"),
    }

    metadata_path = output_dir / "metadata.json"

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return metadata_path

def ingest_series(
    client: bcchapi.Siete,
    series_config: dict[str, Any],
    end_date: str,
    run_started_at: datetime,
) -> None:
    """Ingest a single series from the BCCH API based on the provided configuration."""
    series_name = series_config["name"]
    series_code = series_config["code"]
    start_date = series_config["start_date"]

    # Log the start of the ingestion process
    logger.info(
        "Starting ingestion for %s",
        series_name,
    )

    # Extract the series data from the BCCH API
    df = extract_series(
        client=client,
        series_code=series_code,
        series_name=series_name,
        start_date=start_date,
        end_date=end_date,
    )

    extraction_date = (
        run_started_at.date().isoformat()
    )

    # Build the output directory path for the extracted data
    output_dir = build_output_dir(
        series_name=series_name,
        extraction_date=extraction_date,
    )

    data_path = save_raw(
        df=df,
        output_dir=output_dir,
    )

    # Save metadata about the extracted data
    save_metadata(
        df=df,
        series_config=series_config,
        start_date=start_date,
        end_date=end_date,
        extracted_at=run_started_at,
        output_dir=output_dir,
    )

    # Log the completion of the ingestion process
    logger.info(
        "Completed %s: %s rows written to %s",
        series_name,
        len(df),
        data_path,
    )
    
# =======================
# MAIN
# =======================
def main() -> None:
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

        ingest_series(
            client=client,
            series_config=series_config,
            end_date=end_date,
            run_started_at=run_started_at,
        )
        
if __name__ == "__main__":
    main()