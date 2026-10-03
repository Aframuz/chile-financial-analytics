"""BCCh ingestion pipeline: series metadata + observations for a date window.

    python -m src.pipelines.bcch [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

The window applies to observations; series metadata is a catalog
snapshot and has no dates. Leave start_date unset for regular runs:
BCCh revises history and publishes monthly series about two months
late, so only a full-history window catches every change. Set it for
targeted historical reloads.

The last stdout line is a JSON report (exit code, rows changed, per-step
run ids and summaries) for orchestrators; Airflow pushes it to XCom.
"""

import argparse
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.common.exit_codes import (
    combine_exit_codes,
    exit_code_for_exception,
)

from src.common.logging_config import (
    configure_logging,
    run_context,
)

from src.common.timing import (
    Timings,
)

from src.ingestion import (
    observations,
    series,
)

from src.ingestion.bcch import (
    RunResult,
    make_run_id,
    print_report,
    step_report,
)


logger = logging.getLogger(__spec__.name if __spec__ else __name__)


@dataclass(frozen=True)
class PipelineResult:

    exit_code: int
    report: dict[str, Any]


def run_step(
    run: Callable[[], RunResult],
) -> tuple[int, dict[str, Any]]:
    """Run one step; a crash becomes an exit code, not an exception."""

    try:
        result = run()

    except Exception as exc:
        exit_code = exit_code_for_exception(exc)

        return exit_code, {
            "status": "crashed",
            "exit_code": exit_code,
            # Unknown: the step may have loaded rows before crashing
            "rows_changed": None,
        }

    return result.exit_code, step_report(result)


def run_bcch_pipeline(
    start_date: str | None = None,
    end_date: str | None = None,
) -> PipelineResult:
    """Run both ingestion steps; the worst exit code wins.

    Both steps always run, so one failing doesn't skip the other. The
    pipeline gets its own run id; steps record it as parent_run_id.
    """

    run_id = make_run_id(datetime.now(timezone.utc))

    with run_context(run_id):
        return _run_bcch_pipeline(run_id, start_date, end_date)


def _run_bcch_pipeline(
    run_id: str,
    start_date: str | None,
    end_date: str | None,
) -> PipelineResult:

    logger.info(
        "Starting BCCh pipeline: window=%s → %s",
        start_date or "configured start_date",
        end_date or "today",
    )

    timings = Timings()

    with timings.measure("series"):
        series_step = run_step(series.run_series)

    with timings.measure("observations"):
        observations_step = run_step(
            lambda: observations.run_observations(
                start_date=start_date,
                end_date=end_date,
            )
        )

    steps = {
        "series": series_step,
        "observations": observations_step,
    }

    step_rows = [
        report["rows_changed"]
        for _, report in steps.values()
    ]

    exit_code = combine_exit_codes(
        [code for code, _ in steps.values()]
    )

    rows_changed = (
        None
        if None in step_rows
        else sum(step_rows)
    )

    logger.log(
        logging.INFO if exit_code == 0 else logging.ERROR,
        "BCCh pipeline finished: exit_code=%s, rows_changed=%s, "
        "steps=%s (%s)",
        exit_code,
        rows_changed,
        {name: report["status"] for name, (_, report) in steps.items()},
        timings.describe(),
    )

    return PipelineResult(
        exit_code=exit_code,
        report={
            "run_id": run_id,
            "exit_code": exit_code,
            "start_date": start_date,
            "end_date": end_date,
            "rows_changed": rows_changed,
            "timings_seconds": timings,
            "steps": {
                name: report
                for name, (_, report) in steps.items()
            },
        },
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the BCCh ingestion pipeline.",
    )

    parser.add_argument(
        "--start-date",
        type=observations.iso_date,
        help="First observation date (YYYY-MM-DD). Default: full history.",
    )

    parser.add_argument(
        "--end-date",
        type=observations.iso_date,
        help="Last observation date (YYYY-MM-DD). Default: today (UTC).",
    )

    args = parser.parse_args(argv)

    configure_logging()

    result = run_bcch_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    print_report(result.report)

    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
