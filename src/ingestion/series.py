"""BCCh series metadata pipeline.

BCCh SearchSeries catalog ─┐
                           ├→ validate → canonical transform → raw_bcch.series
metadata/bcch/series.yml ──┘

Grain of raw_bcch.series: one row = latest known
metadata for one BCCh series (unique series_code).

Which series to describe comes from config/bcch_series.yml
(the same enabled series the observations pipeline ingests).
"""

import logging
import os

from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any

import bcchapi
import pandas as pd
import yaml

from src.common.storage import (
    calculate_sha256,
    write_dataframe_json_atomic,
    write_json_atomic,
)

from src.validation.bcch import (
    validate_series_metadata,
)

from src.transforms.bcch import (
    prepare_bcch_series,
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
    run_context,
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
    publish_series_to_bigquery,
)

from src.ingestion.bcch import (
    make_run_id,
    upload_run_summary,
    RunResult,
    print_report,
    step_report,
    CONFIG_PATH,
    RAW_DATA_DIR,
    create_client,
    enabled_series,
    load_config,
    save_run_summary,
    validate_config,
)


# PATHS
CURATED_METADATA_PATH = Path("metadata/bcch/series.yml")

# Artifacts live next to the observation series folders:
# data/raw/bcch/_series/extraction_date=YYYY-MM-DD/
SERIES_METADATA_DIR = "_series"

RUNS_DIR = Path("data/_runs/bcch_series")

# __spec__.name keeps the module path when run with `python -m`
logger = logging.getLogger(__spec__.name if __spec__ else __name__)

# ==================================
# FUNCTIONS
# ==================================

def load_curated_metadata(
    path: Path,
) -> dict[str, dict[str, Any]]:
    """Load curated metadata, keyed by series code."""

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        content = yaml.safe_load(file)

    if not content or "series" not in content:
        raise ValueError(
            "Curated metadata must contain a 'series' section."
        )

    curated: dict[str, dict[str, Any]] = {}

    for entry in content["series"]:

        code = entry.get("code")

        if not code:
            raise ValueError(
                f"Curated metadata entry without code: {entry}"
            )

        if code in curated:
            raise ValueError(
                f"Duplicate curated series code: {code}"
            )

        curated[code] = entry

    return curated

def extract_catalog(
    client: bcchapi.Siete,
    timings: Timings | None = None,
    retry_stats: RetryStats | None = None,
) -> pd.DataFrame:
    """Extract the BCCh SearchSeries catalog for every frequency.

    bcchapi exposes SearchSeries through its underlying
    session; one call per frequency.
    """

    session = client.stat.session

    frames = []

    for frequency in sorted(session.FREQUENCIES):

        logger.info(
            "Extracting BCCh catalog for frequency %s",
            frequency,
        )

        with (timings if timings is not None else Timings()).measure(
            f"extract_{frequency.lower()}"
        ):
            response = call_with_retries(
                lambda: session.search(frequency),
                description=f"BCCh catalog search for {frequency}",
                stats=retry_stats,
            )

        frames.append(
            response.to_df()
        )

    return pd.concat(
        frames,
        ignore_index=True,
    )

def build_output_dir(
    extraction_date: str,
) -> Path:
    """Build the output directory path for the metadata artifacts."""
    return (
        RAW_DATA_DIR
        / SERIES_METADATA_DIR
        / f"extraction_date={extraction_date}"
    )

def save_raw(
    source_records: pd.DataFrame,
    output_dir: Path,
) -> tuple[Path, str]:
    """Save the catalog rows for the configured series, as returned by BCCh."""
    output_path = (
        output_dir / "data.json"
    )

    checksum = write_dataframe_json_atomic(
        df=source_records,
        path=output_path,
    )

    return output_path, checksum

def save_metadata(
    source_records: pd.DataFrame,
    series_configs: list[dict[str, Any]],
    extracted_at: datetime,
    output_dir: Path,
    checksum: str,
    validations: dict[str, dict[str, Any]],
) -> Path:
    """Save extraction metadata and per-series quality results."""
    metadata = {
        "source":
            "Banco Central de Chile - BDE",

        "source_function":
            "SearchSeries",

        "requested_series_codes": [
            series_config["code"]
            for series_config in series_configs
        ],

        "row_count":
            len(source_records),

        "extracted_at_utc":
            extracted_at.isoformat(),

        "extractor":
            "bcchapi",

        "bcchapi_version":
            version("bcchapi"),

        "data_file_sha256":
            checksum,

        "curated_metadata_path":
            str(CURATED_METADATA_PATH),

        "curated_metadata_sha256":
            calculate_sha256(
                CURATED_METADATA_PATH
            ),

        "quality": validations,
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

def ingest_series_metadata(
    client: bcchapi.Siete,
    series_configs: list[dict[str, Any]],
    curated: dict[str, dict[str, Any]],
    run_started_at: datetime,
    timings: Timings | None = None,
    retry_stats: RetryStats | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Ingest metadata for the configured series.

    Returns one result per series, and the warehouse rows changed.
    timings: filled with seconds per step for the whole batch.
    """

    if timings is None:
        timings = Timings()

    with timings.measure("extract"):
        catalog = extract_catalog(
            client,
            timings=timings,
            retry_stats=retry_stats,
        )

    codes = [
        series_config["code"]
        for series_config in series_configs
    ]

    source_records = (
        catalog[catalog["seriesId"].isin(codes)]
        .reset_index(drop=True)
    )

    with timings.measure("validate"):
        validations = {
            series_config["code"]: validate_series_metadata(
                series_config=series_config,
                catalog_matches=source_records[
                    source_records["seriesId"]
                    == series_config["code"]
                ],
                curated=curated.get(
                    series_config["code"]
                ),
            )
            for series_config in series_configs
        }

    extraction_date = (
        run_started_at
        .date()
        .isoformat()
    )

    output_dir = build_output_dir(
        extraction_date=extraction_date,
    )

    with timings.measure("write_raw"):

        data_path, checksum = save_raw(
            source_records=source_records,
            output_dir=output_dir,
        )

        metadata_path = save_metadata(
            source_records=source_records,
            series_configs=series_configs,
            extracted_at=run_started_at,
            output_dir=output_dir,
            checksum=checksum,
            validations=validations,
        )

    with timings.measure("publish"):
        published = publish_artifacts(
            data_path=data_path,
            metadata_path=metadata_path,
            series_name=SERIES_METADATA_DIR,
            extraction_date=extraction_date,
        )

    passed_codes = [
        code
        for code, validation in validations.items()
        if validation["passed"]
    ]

    for code, validation in validations.items():

        if not validation["passed"]:

            logger.error(
                "Metadata validation failed for %s: %s",
                code,
                "; ".join(
                    f"{check['name']} ({check['message']})"
                    for check in validation["checks"]
                    if not check["passed"]
                ),
            )

    warehouse_df = prepare_bcch_series(
        catalog=source_records[
            source_records["seriesId"].isin(passed_codes)
        ],
        curated=curated,
        extracted_at=run_started_at,
    )

    with timings.measure("warehouse"):
        rows_changed = publish_series_to_bigquery(
            warehouse_df=warehouse_df,
            configured_codes=codes,
            timings=timings,
        )

    logger.info(
        "Loaded series metadata: %s/%s series passed validation, "
        "%s rows changed in warehouse, raw at %s (%s)",
        len(passed_codes),
        len(series_configs),
        rows_changed,
        published["data_uri"],
        timings.describe(),
    )

    results = [
        {
            "series_name": series_config["name"],
            "series_code": series_config["code"],
            "status": (
                "success"
                if validations[series_config["code"]]["passed"]
                else "quality_failed"
            ),
            "local_data_path": str(data_path),
            "local_metadata_path": str(metadata_path),
            "data_uri": published["data_uri"],
            "metadata_uri": published["metadata_uri"],
            "checksum_sha256": checksum,
            "quality_passed":
                validations[series_config["code"]]["passed"],
            "warehouse_loaded": (
                bigquery_enabled()
                and series_config["code"] in passed_codes
            ),
        }
        for series_config in series_configs
    ]

    # One MERGE for the whole batch: the count belongs to the run.
    return results, rows_changed

# =======================
# MAIN
# =======================
def main() -> int:

    configure_logging()

    result = run_series()

    print_report(step_report(result))

    return result.exit_code


def run_series() -> RunResult:
    """Extract, validate and load series metadata (a catalog snapshot)."""

    run_started_at = datetime.now(
        timezone.utc
    )

    with run_context(make_run_id(run_started_at)):
        return _run_series(run_started_at)


def _run_series(run_started_at: datetime) -> RunResult:

    rows_changed = 0

    timings = Timings()
    retry_stats = RetryStats()

    config = load_config(CONFIG_PATH)

    validate_config(config)

    series_configs = enabled_series(config)

    curated = load_curated_metadata(
        CURATED_METADATA_PATH
    )

    unused_codes = set(curated) - {
        series_config["code"]
        for series_config in series_configs
    }

    if unused_codes:
        logger.warning(
            "Curated metadata for series not enabled "
            "in config (ignored): %s",
            sorted(unused_codes),
        )

    client = create_client()

    logger.info(
        "Starting series metadata run: series=%s, storage=%s, bigquery=%s",
        [s["name"] for s in series_configs],
        os.getenv("STORAGE_BACKEND", "local").lower(),
        bigquery_enabled(),
    )

    try:

        results, rows_changed = ingest_series_metadata(
            client=client,
            series_configs=series_configs,
            curated=curated,
            run_started_at=run_started_at,
            timings=timings,
            retry_stats=retry_stats,
        )

    except Exception as exc:

        # Metadata is extracted and loaded as one batch:
        # a technical failure affects every series
        logger.exception(
            "Technical failure while "
            "ingesting series metadata (%s)",
            timings.describe() or "before any step finished",
        )

        results = [
            {
                "series_name":
                    series_config["name"],

                "series_code":
                    series_config["code"],

                "status":
                    "technical_failed",

                "error":
                    safe_error_message(exc),

                "retryable":
                    is_retryable(exc),
            }
            for series_config in series_configs
        ]

    ended_at = datetime.now(
        timezone.utc
    )

    summary_path = save_run_summary(
        results=results,
        started_at=run_started_at,
        ended_at=ended_at,
        pipeline="bcch_series_metadata",
        runs_dir=RUNS_DIR,
        details={
            "timings_seconds": timings,
            "api_retries": retry_stats.as_dict(),
        },
        rows_changed=rows_changed,
    )


    return RunResult(
        exit_code=exit_code_for(results),
        summary_path=summary_path,
        summary_uri=upload_run_summary(summary_path),
    )

if __name__ == "__main__":
    raise SystemExit(run_entrypoint(main))
