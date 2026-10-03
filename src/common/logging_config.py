"""Logging for CLI entrypoints: one setup, every line traceable to its run.

    2026-10-03 00:53:30,773 | INFO | src.ingestion.observations
        | run=20261003T005330773049Z parent=20261003T005329… airflow=bcch_financial_pipeline/manual__…/ingest_observations#1
        | Extracting usd_clp ...

run= is the pipeline run id (also the run summary's file name); parent=
is the enclosing run when steps run inside src.pipelines.bcch; airflow=
is added when Airflow passes its context (AIRFLOW_CTX_* variables). A
line thus leads to its run summary, Airflow task try and BigQuery jobs
(which carry the same values as labels, see src/common/bigquery.py).

LOG_FORMAT=json emits one JSON object per line instead, with those
values as fields, for log aggregators such as Cloud Logging.

Entrypoints call configure_logging() once; importing a module never
configures logging.
"""

import json
import logging
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from src.common.redaction import RedactingFormatter

TEXT_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(trace)s | %(message)s"
)

# Noisy at INFO; their problems surface as exceptions anyway. They follow
# LOG_LEVEL=DEBUG, e.g. to inspect HTTP calls.
QUIET_LOGGERS = ("urllib3", "google", "requests")

_run_id: ContextVar[str | None] = ContextVar("run_id", default=None)
_parent_run_id: ContextVar[str | None] = ContextVar("parent_run_id", default=None)


def current_run_id() -> str | None:
    return _run_id.get()


def current_parent_run_id() -> str | None:
    return _parent_run_id.get()


def airflow_context() -> dict[str, str] | None:
    """dag_id, run_id, task_id and try number from Airflow's variables."""

    dag_id = os.getenv("AIRFLOW_CTX_DAG_ID")

    if not dag_id:
        return None

    return {
        "dag_id": dag_id,
        "run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "-"),
        "task_id": os.getenv("AIRFLOW_CTX_TASK_ID", "-"),
        "try_number": os.getenv("AIRFLOW_CTX_TRY_NUMBER", "-"),
    }


class TraceFilter(logging.Filter):
    """Add trace fields: run id, parent run id and Airflow context."""

    def __init__(self) -> None:
        super().__init__()
        self.airflow = airflow_context()

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _run_id.get()
        record.parent_run_id = _parent_run_id.get()
        record.airflow = self.airflow

        trace = f"run={record.run_id or '-'}"

        if record.parent_run_id:
            trace += f" parent={record.parent_run_id}"

        if self.airflow:
            trace += (
                f" airflow={self.airflow['dag_id']}/{self.airflow['run_id']}"
                f"/{self.airflow['task_id']}#{self.airflow['try_number']}"
            )

        record.trace = trace
        return True


class JsonFormatter(logging.Formatter):
    """One JSON object per line; trace values as fields."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": self.formatTime(record),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", None),
            "parent_run_id": getattr(record, "parent_run_id", None),
            "airflow": getattr(record, "airflow", None),
        }

        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, ensure_ascii=False)


@contextmanager
def run_context(run_id: str) -> Iterator[None]:
    """Tag every log line emitted inside the block with `run_id`.

    A run started inside another run (a pipeline step) records the outer
    run id as its parent.
    """

    outer = _run_id.get()
    run_token = _run_id.set(run_id)
    parent_token = _parent_run_id.set(outer)

    try:
        yield
    finally:
        _parent_run_id.reset(parent_token)
        _run_id.reset(run_token)


def configure_logging() -> None:
    """Configure the root logger once.

    LOG_LEVEL (default INFO) and LOG_FORMAT (text | json, default text).
    Output goes to stderr: stdout's last line is reserved for the JSON
    report orchestrators read. Tokens are redacted, tracebacks included.
    """

    root = logging.getLogger()

    if any(getattr(handler, "_bcch", False) for handler in root.handlers):
        return

    level = logging.getLevelName(os.getenv("LOG_LEVEL", "INFO").upper())

    if not isinstance(level, int):
        level = logging.INFO

    inner = (
        JsonFormatter()
        if os.getenv("LOG_FORMAT", "text").lower() == "json"
        else logging.Formatter(TEXT_FORMAT)
    )

    handler = logging.StreamHandler(sys.stderr)
    handler._bcch = True
    handler.addFilter(TraceFilter())
    handler.setFormatter(RedactingFormatter(inner))

    root.addHandler(handler)
    root.setLevel(level)

    for name in QUIET_LOGGERS:
        logging.getLogger(name).setLevel(
            level if level <= logging.DEBUG else logging.WARNING
        )
