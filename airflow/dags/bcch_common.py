"""Shared helpers for the BCCh DAGs (not a DAG itself).

Tasks run the project's CLI entrypoints as subprocesses; the exit code
decides whether Airflow retries (transient) or fails immediately. The
entrypoints' last stdout line is a JSON report, which tasks return so
Airflow stores it as an XCom: small metadata only, never data.
"""

import json
import logging
import os
import re
from collections import Counter
from typing import Any

from airflow.providers.standard.hooks.subprocess import (
    SubprocessHook,
    SubprocessResult,
)
from airflow.sdk import Variable, get_current_context
from airflow.sdk.exceptions import AirflowException, AirflowFailException

log = logging.getLogger(__name__)

# src/ resolves config/, metadata/ and data/ relative to the project root.
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/project")

# One slot (created by airflow-init in docker-compose.airflow.yml), shared by
# every task that writes or reads raw_bcch. It stops overlapping MERGEs, and
# stops dbt reading half-loaded raw data: a backfill row committed after
# the fact model ran, with an extracted_at already covered, would be skipped.
BIGQUERY_POOL = "bcch_bigquery"

INGESTION_RETRYABLE_EXIT_CODES = {75}  # EX_TEMPFAIL, see src/common/exit_codes.py

# "raw_bcch changed since the last successful dbt transform". An XCom only
# lives within one run, but changes can come from a backfill or from a run
# whose dbt step failed; the next quiet run must still rebuild the marts.
MARTS_STALE_VARIABLE = "bcch_marts_stale"


def airflow_context_env() -> dict[str, str]:
    """Airflow's run context as AIRFLOW_CTX_* variables for a subprocess.

    src/common/logging_config.py adds them to every log line, linking
    the subprocess's logs to this task try. Airflow doesn't export them
    to child processes by itself.
    """

    ti = get_current_context()["ti"]

    return {
        "AIRFLOW_CTX_DAG_ID": ti.dag_id,
        "AIRFLOW_CTX_DAG_RUN_ID": ti.run_id,
        "AIRFLOW_CTX_TASK_ID": ti.task_id,
        "AIRFLOW_CTX_TRY_NUMBER": str(ti.try_number),
    }


def run_command(command: str) -> SubprocessResult:
    """Run `command` in the project dir, streaming output to the task log.

    Returns the exit code and the last output line.
    """

    hook = SubprocessHook()

    try:
        result = hook.run_command(
            command=["bash", "-c", command],
            cwd=PROJECT_DIR,
            env={**os.environ, **airflow_context_env()},
        )
    except BaseException:
        # e.g. execution_timeout: don't leave the child running.
        hook.send_sigterm()
        raise

    return result


def raise_for_exit_codes(exit_codes: list[int], retryable: set[int]) -> None:
    """Fail the task; retry only if every failure is retryable."""

    failures = [code for code in exit_codes if code != 0]

    if not failures:
        return

    if all(code in retryable for code in failures):
        raise AirflowException(
            f"Transient failure (exit codes {failures}); will retry."
        )

    raise AirflowFailException(
        f"Permanent failure (exit codes {failures}); not retrying."
    )


def parse_report(output: str) -> dict[str, Any] | None:
    """Parse the JSON report an entrypoint prints as its last line."""

    try:
        report = json.loads(output)
    except (TypeError, ValueError):
        report = None

    if not isinstance(report, dict):
        log.warning("No JSON report on the last output line: %r", output)
        return None

    return report


def mark_marts_stale(rows_changed: int | None) -> None:
    """Flag the marts for a rebuild if raw data changed (or may have)."""

    if rows_changed is None or rows_changed > 0:
        Variable.set(MARTS_STALE_VARIABLE, "true")


def marts_stale() -> bool:
    return Variable.get(MARTS_STALE_VARIABLE, default="false") == "true"


def mark_marts_fresh() -> None:
    Variable.set(MARTS_STALE_VARIABLE, "false")


def dbt_run_paths() -> dict[str, str]:
    """Per task-try dbt target and log folders.

    dbt otherwise overwrites target/run_results.json on every command and
    appends every run to one logs/dbt.log; separate folders keep each
    task try's results, compiled SQL and logs. They accumulate under
    target/airflow/ and need periodic cleanup.
    """

    ti = get_current_context()["ti"]
    run = re.sub(r"[^A-Za-z0-9_.-]", "_", ti.run_id)

    base = os.path.join(
        os.environ["DBT_PROJECT_DIR"],
        "target",
        "airflow",
        ti.dag_id,
        run,
        f"{ti.task_id}_try{ti.try_number}",
    )

    return {
        "target_path": base,
        "log_path": os.path.join(base, "logs"),
    }


def dbt_summary(command: str, target_path: str) -> dict[str, Any]:
    """Status counts and invocation id from dbt's run_results.json, for XCom.

    invocation_id also labels the BigQuery jobs dbt ran (see
    macros/bcch_query_comment.sql), linking this task to them.
    """

    path = os.path.join(target_path, "run_results.json")

    with open(path, encoding="utf-8") as file:
        run_results = json.load(file)

    return {
        "command": command,
        "invocation_id": run_results["metadata"]["invocation_id"],
        "elapsed_seconds": round(run_results["elapsed_time"], 1),
        "statuses": dict(
            Counter(node["status"] for node in run_results["results"])
        ),
        "target_path": target_path,
    }
