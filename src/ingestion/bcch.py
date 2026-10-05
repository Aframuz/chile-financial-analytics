"""Shared BCCh ingestion utilities.

Used by both BCCh pipelines:

- src.ingestion.observations → raw_bcch.observations
- src.ingestion.series       → raw_bcch.series
"""

import json
import logging
import os
import uuid

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import bcchapi
import yaml

from dotenv import load_dotenv

from src.common.logging_config import (
    current_parent_run_id,
    run_context,
)

from src.common.publisher import (
    publish_run_summary,
)

from src.common.redaction import (
    safe_error_message,
)

from src.common.storage import (
    write_json_atomic,
)

from src.common.warehouse import (
    record_run_completed,
    record_run_crashed,
    record_run_started,
)


logger = logging.getLogger(__name__)

# PATHS
CONFIG_PATH = Path("config/bcch_series.yml")
RAW_DATA_DIR = Path("data/raw/bcch")

# Audit records keep a short error; the run summary has the detail
MAX_ERROR_MESSAGE_LENGTH = 1000
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

def make_run_id(started_at: datetime) -> str:
    """Unique id of one execution, also the run summary's file name.

    The start timestamp keeps ids sortable by time; the random suffix
    makes them unique even for runs started in the same microsecond.
    Runs before the suffix was added have the timestamp only.
    """

    return (
        started_at.strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )


@dataclass
class RowCounts:
    """Rows through each stage of one series (or batch), kept as it goes.

    Passed into a step like Timings, so a step that raises midway still
    reports how far its rows got.
    """

    extracted: int = 0
    valid: int = 0
    rejected: int = 0
    loaded: int = 0

    def as_dict(self) -> dict[str, int]:
        """Result fields summed by save_run_summary."""

        return {
            "row_count": self.extracted,
            "valid_rows": self.valid,
            "rejected_rows": self.rejected,
            "loaded_rows": self.loaded,
        }


def short_error_message(message: str | None) -> str | None:
    """Truncate an error for audit records (already redacted)."""

    if not message or len(message) <= MAX_ERROR_MESSAGE_LENGTH:
        return message

    suffix = " … (truncated; see run summary)"

    return message[: MAX_ERROR_MESSAGE_LENGTH - len(suffix)] + suffix


def run_error_message(results: list[dict[str, Any]]) -> str | None:
    """One line naming each failed series and why, or None if none failed."""

    errors = []

    for result in results:

        if result["status"] == "success":
            continue

        reason = (
            result.get("error")
            or ", ".join(result.get("failed_checks") or [])
            or "no detail"
        )

        errors.append(
            f"{result.get('series_name')}: {result['status']} ({reason})"
        )

    return short_error_message("; ".join(errors)) if errors else None


def save_run_summary(
    results: list[dict[str, Any]],
    run_id: str,
    started_at: datetime,
    ended_at: datetime,
    pipeline: str,
    runs_dir: Path,
    details: dict[str, Any] | None = None,
    rows_changed: int | None = None,
    row_counts: RowCounts | None = None,
) -> Path:
    """Write a JSON summary of a pipeline run, one result per series.

    Row counts, summed over results (see RowCounts.as_dict):
    extracted_rows fetched from BCCh, valid_rows / rejected_rows that
    passed / failed validation, loaded_rows written to the warehouse.
    row_counts: batch counts instead, when results don't carry them.

    rows_changed: warehouse rows the run changed; defaults to the sum
    of the per-series values.
    """

    if rows_changed is None:
        rows_changed = sum(
            result.get("rows_changed") or 0
            for result in results
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
        # Set when run as a step of src.pipelines.bcch
        "parent_run_id": current_parent_run_id(),
        "pipeline": pipeline,

        "status":
            overall_status,

        "started_at_utc":
            started_at.isoformat(),

        "ended_at_utc":
            ended_at.isoformat(),

        "duration_seconds":
            round((ended_at - started_at).total_seconds(), 3),

        **(details or {}),

        **{
            name: (
                row_counts.as_dict()[result_field]
                if row_counts is not None
                else sum(
                    result.get(result_field) or 0
                    for result in results
                )
            )
            for name, result_field in (
                ("extracted_rows", "row_count"),
                ("valid_rows", "valid_rows"),
                ("rejected_rows", "rejected_rows"),
                ("loaded_rows", "loaded_rows"),
            )
        },

        "rows_changed":
            rows_changed,

        "error_message":
            run_error_message(results),

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

    logger.log(
        logging.INFO if overall_status == "success" else logging.ERROR,
        "Run finished: status=%s, success=%s, quality_failed=%s, "
        "technical_failed=%s, rows_changed=%s, duration=%.1fs, summary=%s",
        overall_status,
        success_count,
        quality_failed_count,
        technical_failed_count,
        rows_changed,
        summary["duration_seconds"],
        output_path,
    )

    return output_path


@dataclass(frozen=True)
class RunResult:
    """Outcome of one ingestion run: exit code + where its summary is."""

    exit_code: int
    summary_path: Path
    summary_uri: str | None = None


@dataclass(frozen=True)
class IngestionRun:
    """Identity of one execution, shared by everything it writes."""

    run_id: str
    started_at: datetime
    pipeline: str
    table_name: str


@contextmanager
def ingestion_run(pipeline: str, table_name: str) -> Iterator[IngestionRun]:
    """Start an execution: new run id, log context, ingestion_runs row.

    The row is written as running before any data; finish_run completes
    it. A run that raises is marked crashed; one killed outright stays
    running.

    table_name: the raw table the run loads (e.g. "observations").
    """

    started_at = datetime.now(timezone.utc)

    run = IngestionRun(
        run_id=make_run_id(started_at),
        started_at=started_at,
        pipeline=pipeline,
        table_name=table_name,
    )

    with run_context(run.run_id):

        record_run_started(
            run_id=run.run_id,
            pipeline=pipeline,
            table_name=table_name,
            started_at=started_at,
        )

        try:
            yield run

        except Exception as exc:
            record_run_crashed(
                run_id=run.run_id,
                pipeline=pipeline,
                table_name=table_name,
                started_at=started_at,
                error_message=short_error_message(
                    f"{type(exc).__name__}: {safe_error_message(exc)}"
                ),
            )
            raise


def finish_run(
    run: IngestionRun,
    summary_path: Path,
    exit_code: int,
) -> RunResult:
    """Publish the run summary and complete the run's ingestion_runs row."""

    summary_uri = upload_run_summary(summary_path)

    record_run_completed(
        summary=json.loads(summary_path.read_text(encoding="utf-8")),
        table_name=run.table_name,
        exit_code=exit_code,
        summary_uri=summary_uri,
    )

    return RunResult(
        exit_code=exit_code,
        summary_path=summary_path,
        summary_uri=summary_uri,
    )


def upload_run_summary(summary_path: Path) -> str | None:
    """Copy the run summary next to the raw data (GCS when configured).

    Observability, not data: a failed upload is logged, not fatal; the
    local summary still exists.
    """

    try:
        return publish_run_summary(summary_path)

    except Exception:
        logger.exception(
            "Could not upload run summary %s",
            summary_path,
        )
        return None


def step_report(result: RunResult) -> dict[str, Any]:
    """Small, orchestrator-facing view of a run summary."""

    summary = json.loads(
        result.summary_path.read_text(encoding="utf-8")
    )

    return {
        "run_id": summary["run_id"],
        "pipeline": summary["pipeline"],
        "status": summary["status"],
        "exit_code": result.exit_code,
        "rows_changed": summary["rows_changed"],
        "duration_seconds": summary["duration_seconds"],
        "parent_run_id": summary["parent_run_id"],
        # Batch-level (series) or summed per series (observations)
        "api_retries": (
            summary["api_retries"]["retries"]
            if "api_retries" in summary
            else sum(
                (result.get("api_retries") or {}).get("retries", 0)
                for result in summary["results"]
            )
        ),
        "summary_path": str(result.summary_path),
        "summary_uri": result.summary_uri,
    }


def print_report(report: dict[str, Any]) -> None:
    """Print the report as the process's last stdout line.

    Orchestrators read it (Airflow pushes it to XCom); logs go to stderr.
    """

    print(json.dumps(report), flush=True)
