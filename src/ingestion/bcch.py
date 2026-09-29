"""Shared BCCh ingestion utilities.

Used by both BCCh pipelines:

- src.ingestion.observations → raw_bcch.observations
- src.ingestion.series       → raw_bcch.series
"""

import os

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import bcchapi
import yaml

from dotenv import load_dotenv

from src.common.storage import (
    write_json_atomic,
)


# PATHS
CONFIG_PATH = Path("config/bcch_series.yml")
RAW_DATA_DIR = Path("data/raw/bcch")
RUNS_DIR = Path("data/_runs/bcch")

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
    seen_codes = set()

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

        # series_code is the natural key of both
        # raw_bcch.observations and raw_bcch.series
        series_code = series["code"]

        if series_code in seen_codes:
            raise ValueError(
                f"Duplicate series code: {series_code}"
            )

        seen_codes.add(series_code)

def enabled_series(
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return the series configurations that are enabled."""
    return [
        series_config
        for series_config in config["series"]
        if series_config.get("enabled", True)
    ]

def resolve_end_date(
    configured_end_date: str | None,
) -> str:
    """Resolve the end date for data ingestion.
    Using UTC gives us one consistent technical clock.
    """
    if configured_end_date:
        return configured_end_date

    return datetime.now(timezone.utc).date().isoformat()

def save_run_summary(
    results: list[dict[str, Any]],
    started_at: datetime,
    ended_at: datetime,
    pipeline: str,
    runs_dir: Path,
    details: dict[str, Any] | None = None,
) -> Path:
    """Write a JSON summary of a pipeline run, one result per series."""

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
        "pipeline": pipeline,

        "status":
            overall_status,

        "started_at_utc":
            started_at.isoformat(),

        "ended_at_utc":
            ended_at.isoformat(),

        **(details or {}),

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
        runs_dir
        / f"run_date={run_date}"
        / f"{run_id}.json"
    )

    write_json_atomic(
        data=summary,
        path=output_path,
    )

    return output_path
