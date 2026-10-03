import logging

import pytest

from src.common.logging_config import (
    TraceFilter,
    run_context,
)
from src.common.timing import Timings


def make_record():

    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="hello",
        args=(),
        exc_info=None,
    )


def test_trace_has_run_id_inside_run_context(monkeypatch):

    monkeypatch.delenv("AIRFLOW_CTX_DAG_ID", raising=False)
    trace_filter = TraceFilter()

    record = make_record()
    trace_filter.filter(record)
    assert record.trace == "run=-"

    with run_context("20261003T000000000000Z"):
        record = make_record()
        trace_filter.filter(record)

    assert record.trace == "run=20261003T000000000000Z"


def test_trace_includes_airflow_context(monkeypatch):

    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "bcch_financial_pipeline")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "manual__x")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "ingest_series")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "2")

    record = make_record()

    with run_context("r1"):
        TraceFilter().filter(record)

    assert record.trace == (
        "run=r1 airflow=bcch_financial_pipeline/manual__x/ingest_series#2"
    )


def test_timings_measure_steps_even_when_they_raise():

    timings = Timings()

    with timings.measure("extract"):
        pass

    with pytest.raises(RuntimeError):
        with timings.measure("warehouse"):
            raise RuntimeError("boom")

    assert set(timings) == {"extract", "warehouse"}
    assert all(seconds >= 0 for seconds in timings.values())
    assert timings.describe().startswith("extract 0.")


def test_nested_run_records_parent(monkeypatch):

    monkeypatch.delenv("AIRFLOW_CTX_DAG_ID", raising=False)
    record = make_record()

    with run_context("pipeline-run"):
        with run_context("step-run"):
            TraceFilter().filter(record)

    assert record.trace == "run=step-run parent=pipeline-run"
    assert record.parent_run_id == "pipeline-run"


def test_json_formatter_emits_trace_fields(monkeypatch):

    import json

    from src.common.logging_config import JsonFormatter

    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag")
    record = make_record()

    with run_context("r1"):
        TraceFilter().filter(record)

    entry = json.loads(JsonFormatter().format(record))

    assert entry["message"] == "hello"
    assert entry["severity"] == "INFO"
    assert entry["run_id"] == "r1"
    assert entry["airflow"]["dag_id"] == "dag"


def test_debug_level_reaches_library_loggers(monkeypatch):

    from src.common import logging_config

    root = logging.getLogger()
    before = (root.level, list(root.handlers))

    try:
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        logging_config.configure_logging()
        assert logging.getLogger("urllib3").level == logging.DEBUG
    finally:
        root.handlers[:] = before[1]
        root.setLevel(before[0])
        logging.getLogger("urllib3").setLevel(logging.NOTSET)


def test_job_labels_are_valid_bigquery_labels(monkeypatch):

    from src.common.bigquery import job_labels

    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "bcch_financial_pipeline")
    monkeypatch.setenv(
        "AIRFLOW_CTX_DAG_RUN_ID",
        "manual__2026-10-03T01:02:13.472000+00:00",
    )
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "ingest_series")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "1")

    with run_context("20261003T010218326696Z"):
        labels = job_labels()

    assert labels["run_id"] == "20261003t010218326696z"
    assert labels["airflow_run_id"] == "manual__2026-10-03t01_02_13_472000_00_00"

    import re

    for key, value in labels.items():
        assert re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", key)
        assert re.fullmatch(r"[a-z0-9_-]{0,63}", value)
