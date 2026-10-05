"""BCCh observations pipeline.

BCCh → validate → data/raw/bcch/{series_name}/ → GCS → raw_bcch.observations
"""

import argparse
import logging
import os

from datetime import date, datetime, timezone
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

from src.common.exit_codes import (
    exit_code_for,
    is_retryable,
    run_entrypoint,
)

from src.common.logging_config import (
    configure_logging,
)

from src.common.redaction import (
    safe_error_message,
)

from src.common.timing import (
    Timings,
)

from src.common.retry import (
    RetryStats,
    call_with_retries,
)

from src.common.warehouse import (
    bigquery_enabled,
    prune_unconfigured_observations,
    publish_to_bigquery,
)

from src.ingestion.bcch import (
    CONFIG_PATH,
    RAW_DATA_DIR,
    RUNS_DIR,
    create_client,
    enabled_series,
    RunResult,
    IngestionRun,
    RowCounts,
    finish_run,
    ingestion_run,
    load_config,
    print_report,
    step_report,
    resolve_end_date,
    save_run_summary,
    validate_config,
)


# __spec__.name keeps the module path when run with `python -m`
logger = logging.getLogger(__spec__.name if __spec__ else __name__)

# Pipeline name in run summaries and raw_bcch.ingestion_runs
PIPELINE = "bcch_ingestion"

# ==================================
# FUNCTIONS
# ==================================

def extract_series(
    client: bcchapi.Siete,
    series_code: str,
    series_name: str,
    start_date: str,
    end_date: str,
    retry_stats: RetryStats | None = None,
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
    df = call_with_retries(
        lambda: client.cuadro(
            series=[series_code],
            nombres=[series_name],
            desde=start_date,
            hasta=end_date,
        ),
        description=f"BCCh extraction of {series_name}",
        stats=retry_stats,
    )

    return df

def build_output_dir(
    series_name: str,
    extraction_date: str,
    window: str | None = None,
) -> Path:
    """Build the output directory path for the extracted data.

    Explicit windows (backfills, reloads) get their own subfolder so
    several windows extracted on the same day don't overwrite each other.
    """

    output_dir = (
        RAW_DATA_DIR
        / series_name
        / f"extraction_date={extraction_date}"
    )

    if window:
        output_dir = output_dir / f"window={window}"

    return output_dir

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
    ingestion_run_id: str,
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

        "ingestion_run_id":
            ingestion_run_id,

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
    run: IngestionRun,
    window: str | None = None,
    timings: Timings | None = None,
    retry_stats: RetryStats | None = None,
    counts: RowCounts | None = None,
) -> dict[str, Any]:
    """Ingest a single series based on the provided configuration.

    run: the execution this belongs to; its id is written on every
    warehouse row (ingestion_run_id).

    window: set for an explicitly requested date window ("START_END").
    Raw files then land in a window= subfolder, and an empty extraction
    is a valid "no data" result rather than a quality failure (e.g. a
    monthly series with no observation dated inside a short window).

    timings: filled with seconds per step (extract, validate, write_raw,
    publish, warehouse); pass one in to keep them if a step raises.
    retry_stats: BCCh API retries and waits, likewise.
    counts: rows extracted, validated and loaded so far, likewise.
    """

    if timings is None:
        timings = Timings()

    if retry_stats is None:
        retry_stats = RetryStats()

    if counts is None:
        counts = RowCounts()

    series_name = (
        series_config["name"]
    )

    series_code = (
        series_config["code"]
    )

    start_date = (
        series_config["start_date"]
    )

    with timings.measure("extract"):
        df = extract_series(
            client=client,
            series_code=series_code,
            series_name=series_name,
            start_date=start_date,
            end_date=end_date,
            retry_stats=retry_stats,
        )

    counts.extracted = len(df)

    if df.empty and window is not None:

        logger.info(
            "No observations for %s between %s and %s (%s)",
            series_name,
            start_date,
            end_date,
            timings.describe(),
        )

        return {
            "series_name": series_name,
            "series_code": series_code,
            "status": "success",
            **counts.as_dict(),
            "rows_changed": 0,
            "timings_seconds": timings,
            "api_retries": retry_stats.as_dict(),
        }

    with timings.measure("validate"):
        validation = validate_series_data(
            df=df,
            series_config=series_config,
            start_date=start_date,
            end_date=end_date,
        )

    # Validation is per series: it accepts or rejects all its rows
    if validation["passed"]:
        counts.valid = len(df)
    else:
        counts.rejected = len(df)

    extraction_date = (
        run.started_at
        .date()
        .isoformat()
    )

    output_dir = build_output_dir(
        series_name=series_name,
        extraction_date=extraction_date,
        window=window,
    )

    with timings.measure("write_raw"):

        data_path, checksum = save_raw(
            df=df,
            output_dir=output_dir,
        )

        metadata_path = save_metadata(
            df=df,
            series_config=series_config,
            start_date=start_date,
            end_date=end_date,
            extracted_at=run.started_at,
            ingestion_run_id=run.run_id,
            output_dir=output_dir,
            checksum=checksum,
            validation=validation,
        )

    with timings.measure("publish"):
        published = publish_artifacts(
            data_path=data_path,
            metadata_path=metadata_path,
            series_name=series_name,
            extraction_date=extraction_date,
            window=window,
        )

    status = (
        "success"
        if validation["passed"]
        else "quality_failed"
    )

    rows_changed = 0

    if validation["passed"]:

        with timings.measure("warehouse"):
            rows_changed = publish_to_bigquery(
                df=df,
                series_config=series_config,
                extracted_at=run.started_at,
                ingestion_run_id=run.run_id,
                timings=timings,
            )

        # The MERGE committed every validated row (one per date)
        if bigquery_enabled():
            counts.loaded = len(df)

        logger.info(
            "Loaded %s: %s rows extracted, %s changed in warehouse, "
            "raw at %s (%s)",
            series_name,
            len(df),
            rows_changed,
            published["data_uri"],
            timings.describe(),
        )

    else:

        # Raw files are kept as evidence; nothing reaches the warehouse.
        logger.error(
            "Data-quality validation failed for %s: %s",
            series_name,
            "; ".join(
                f"{check['name']} ({check['message']})"
                for check in validation["checks"]
                if not check["passed"]
            ),
        )

    return {
        "series_name": series_name,
        "series_code": series_code,
        "status": status,
        **counts.as_dict(),
        "failed_checks": [
            check["name"]
            for check in validation["checks"]
            if not check["passed"]
        ],
        "local_data_path": str(data_path),
        "local_metadata_path": str(metadata_path),
        "data_uri": published["data_uri"],
        "metadata_uri": published["metadata_uri"],
        "checksum_sha256": checksum,
        "quality_passed":validation["passed"],
        "rows_changed": rows_changed,
        "timings_seconds": timings,
        "api_retries": retry_stats.as_dict(),
    }

# =======================
# MAIN
# =======================
def iso_date(value: str) -> str:
    """Validate a YYYY-MM-DD string and return it unchanged."""

    return date.fromisoformat(value).isoformat()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest BCCh observations.",
    )

    parser.add_argument(
        "--start-date",
        type=iso_date,
        help=(
            "First observation date to request (YYYY-MM-DD) for every "
            "series. Defaults to each series' configured start_date "
            "(full history, which also picks up revisions and late "
            "monthly values)."
        ),
    )

    parser.add_argument(
        "--end-date",
        type=iso_date,
        help=(
            "Last observation date to request (YYYY-MM-DD). "
            "Overrides defaults.end_date in the config; "
            "defaults to today (UTC)."
        ),
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    configure_logging()

    result = run_observations(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    print_report(step_report(result))

    return result.exit_code


def run_observations(
    start_date: str | None = None,
    end_date: str | None = None,
) -> RunResult:
    """Extract, validate and load observations for a date window.

    start_date: None re-extracts each series from its configured
    start_date. end_date: None uses defaults.end_date, else today (UTC).
    Returns the exit code (see src/common/exit_codes.py) and summary path.
    """

    with ingestion_run(
        pipeline=PIPELINE,
        table_name="observations",
    ) as run:
        return _run_observations(
            start_date=start_date,
            end_date=end_date,
            run=run,
        )


def _run_observations(
    start_date: str | None,
    end_date: str | None,
    run: IngestionRun,
) -> RunResult:

    if start_date is not None:
        start_date = iso_date(start_date)

    if end_date is not None:
        end_date = iso_date(end_date)

    results: list[dict[str, Any]] = []

    config = load_config(CONFIG_PATH)

    validate_config(config)

    client = create_client()

    configured_end_date = (
        config
        .get("defaults", {})
        .get("end_date")
    )

    end_date = resolve_end_date(
        end_date or configured_end_date
    )

    if start_date is not None and start_date > end_date:
        raise ValueError(
            f"start_date {start_date} is after end_date {end_date}."
        )

    logger.info(
        "Starting observations run: window=%s → %s, series=%s, "
        "storage=%s, bigquery=%s",
        start_date or "configured start_date",
        end_date,
        [s["name"] for s in enabled_series(config)],
        os.getenv("STORAGE_BACKEND", "local").lower(),
        bigquery_enabled(),
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

        timings = Timings()
        retry_stats = RetryStats()
        counts = RowCounts()

        try:

            if start_date is not None:
                series_config = {
                    **series_config,
                    "start_date": start_date,
                }

            result = ingest_series(
                client=client,
                series_config=series_config,
                end_date=end_date,
                run=run,
                window=(
                    f"{start_date}_{end_date}"
                    if start_date is not None
                    else None
                ),
                timings=timings,
                retry_stats=retry_stats,
                counts=counts,
            )

            results.append(result)

        except Exception as exc:

            logger.exception(
                "Technical failure while "
                "ingesting %s (%s)",
                series_config["name"],
                timings.describe() or "before any step finished",
            )

            results.append(
                {
                    "series_name":
                        series_config["name"],

                    "series_code":
                        series_config["code"],

                    "status":
                        "technical_failed",

                    # How far its rows got before the failure
                    **counts.as_dict(),

                    "error":
                        safe_error_message(exc),

                    "retryable":
                        is_retryable(exc),

                    "timings_seconds":
                        timings,

                    "api_retries":
                        retry_stats.as_dict(),
                }
            )

    configured_codes = [
        series_config["code"]
        for series_config in enabled_series(config)
    ]

    pruned_rows = 0

    try:

        pruned_rows = prune_unconfigured_observations(
            configured_codes
        )

        if bigquery_enabled():
            logger.info(
                "Pruned observations of unconfigured series: "
                "%s rows deleted",
                pruned_rows,
            )

    except Exception as exc:

        logger.exception(
            "Technical failure while pruning "
            "unconfigured series"
        )

        results.append(
            {
                "series_name": "_prune_unconfigured",
                "series_code": None,
                "status": "technical_failed",
                "error": safe_error_message(exc),
                "retryable": is_retryable(exc),
            }
        )

    ended_at = datetime.now(
    timezone.utc
    )

    summary_path = save_run_summary(
        results=results,
        run_id=run.run_id,
        started_at=run.started_at,
        ended_at=ended_at,
        pipeline=PIPELINE,
        runs_dir=RUNS_DIR,
        details={
            "requested_start_date": start_date,
            "requested_end_date": end_date,
        },
        rows_changed=(
            sum(
                result.get("rows_changed") or 0
                for result in results
            )
            + pruned_rows
        ),
    )

    return finish_run(
        run=run,
        summary_path=summary_path,
        exit_code=exit_code_for(results),
    )

if __name__ == "__main__":
    raise SystemExit(run_entrypoint(main))
