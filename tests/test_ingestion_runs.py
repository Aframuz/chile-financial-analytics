import re
from datetime import datetime, timezone

import pytest

from src.common.bigquery import (
    INGESTION_RUNS_SCHEMA,
    build_run_upsert_query,
)
from src.common.warehouse import RUN_COMPLETION_COLUMNS
from src.ingestion import bcch
from src.ingestion.bcch import (
    MAX_ERROR_MESSAGE_LENGTH,
    RowCounts,
    finish_run,
    ingestion_run,
    make_run_id,
    run_error_message,
    save_run_summary,
    short_error_message,
)


STARTED_AT = datetime(2026, 10, 5, 22, 57, 11, 123456, tzinfo=timezone.utc)


# ----------------------------
# Run id
# ----------------------------

def test_run_id_is_sortable_timestamp_plus_random_suffix():

    run_id = make_run_id(STARTED_AT)

    assert re.fullmatch(r"20261005T225711123456Z-[0-9a-f]{8}", run_id)


def test_runs_started_together_get_different_ids():

    assert make_run_id(STARTED_AT) != make_run_id(STARTED_AT)


# ----------------------------
# ingestion_runs upserts
# ----------------------------

def test_run_start_only_inserts():

    query = build_run_upsert_query(
        table_id="p.raw_bcch.ingestion_runs",
        columns=["run_id", "status", "started_at"],
        update_columns=[],
    )

    assert "ON target.run_id = source.run_id" in query
    assert "WHEN NOT MATCHED THEN" in query
    # Rerunning the start never overwrites a completed run
    assert "WHEN MATCHED" not in query


def test_run_completion_updates_only_completion_columns():

    columns = [field.name for field in INGESTION_RUNS_SCHEMA]

    query = build_run_upsert_query(
        table_id="p.raw_bcch.ingestion_runs",
        columns=columns,
        update_columns=RUN_COMPLETION_COLUMNS,
    )

    assert "status = source.status" in query
    assert "loaded_rows = source.loaded_rows" in query
    assert "error_message = source.error_message" in query
    # Identity is set once, at start
    assert "started_at = source.started_at" not in query
    assert "run_id = source.run_id," not in query
    # Still inserts the run if its start wasn't recorded
    assert "WHEN NOT MATCHED THEN" in query


def test_completion_columns_exist_in_schema():

    names = {field.name for field in INGESTION_RUNS_SCHEMA}

    assert set(RUN_COMPLETION_COLUMNS) <= names


# ----------------------------
# Run lifecycle
# ----------------------------

@pytest.fixture
def recorded(monkeypatch):

    calls = []

    for name in ("record_run_started", "record_run_completed", "record_run_crashed"):
        monkeypatch.setattr(
            bcch,
            name,
            lambda _name=name, **kwargs: calls.append((_name, kwargs)),
        )

    monkeypatch.setattr(bcch, "upload_run_summary", lambda path: "gs://b/run.json")

    return calls


def test_run_is_recorded_at_start_and_completion(tmp_path, recorded):

    with ingestion_run(pipeline="bcch_ingestion", table_name="observations") as run:

        summary_path = save_run_summary(
            results=[{
                "status": "success",
                "row_count": 4,
                "valid_rows": 4,
                "rejected_rows": 0,
                "loaded_rows": 4,
                "rows_changed": 1,
            }],
            run_id=run.run_id,
            started_at=run.started_at,
            ended_at=datetime.now(timezone.utc),
            pipeline=run.pipeline,
            runs_dir=tmp_path,
        )

        result = finish_run(run, summary_path, exit_code=0)

    (started, start_args), (completed, end_args) = recorded

    assert started == "record_run_started"
    assert start_args["run_id"] == run.run_id
    assert start_args["table_name"] == "observations"

    assert completed == "record_run_completed"
    assert end_args["summary"]["run_id"] == run.run_id
    assert end_args["summary"]["extracted_rows"] == 4
    assert end_args["summary"]["loaded_rows"] == 4
    assert end_args["summary"]["error_message"] is None
    assert end_args["summary_uri"] == "gs://b/run.json"

    # The summary file is named after the same id
    assert summary_path.name == f"{run.run_id}.json"
    assert result.summary_uri == "gs://b/run.json"


def test_run_that_raises_is_recorded_as_crashed(recorded):

    with pytest.raises(RuntimeError):
        with ingestion_run(pipeline="bcch_series_metadata", table_name="series"):
            raise RuntimeError("boom")

    assert [name for name, _ in recorded] == [
        "record_run_started",
        "record_run_crashed",
    ]
    assert recorded[0][1]["run_id"] == recorded[1][1]["run_id"]
    # Failures are audited with a short, typed message
    assert recorded[1][1]["error_message"] == "RuntimeError: boom"


# ----------------------------
# Error messages
# ----------------------------

def test_run_error_message_names_each_failed_series():

    message = run_error_message([
        {"series_name": "usd_clp", "status": "success"},
        {"series_name": "tpm", "status": "quality_failed", "failed_checks": ["min_value", "max_null_ratio"]},
        {"series_name": "imacec", "status": "technical_failed", "error": "Read timed out"},
    ])

    assert message == (
        "tpm: quality_failed (min_value, max_null_ratio); "
        "imacec: technical_failed (Read timed out)"
    )


def test_run_error_message_is_none_without_failures():

    assert run_error_message([{"series_name": "tpm", "status": "success"}]) is None


def test_error_messages_are_capped():

    message = short_error_message("x" * 50_000)

    assert len(message) == MAX_ERROR_MESSAGE_LENGTH
    assert message.endswith("(truncated; see run summary)")


def test_crash_error_is_capped_and_redacted(recorded):

    with pytest.raises(ValueError):
        with ingestion_run(pipeline="bcch_ingestion", table_name="observations"):
            raise ValueError("GET https://si3.bcentral.cl/?token=s3cret " + "y" * 5000)

    message = recorded[1][1]["error_message"]

    assert len(message) == MAX_ERROR_MESSAGE_LENGTH
    assert "s3cret" not in message
    assert "token=***" in message


# ----------------------------
# Row counts survive failures
# ----------------------------

def test_failed_load_keeps_counts_reached(monkeypatch, tmp_path):
    """A series whose warehouse load raises still reports its rows."""

    import pandas as pd

    from src.ingestion import observations

    monkeypatch.setattr(observations, "RAW_DATA_DIR", tmp_path)
    monkeypatch.setattr(
        observations,
        "publish_artifacts",
        lambda **kwargs: {"data_uri": None, "metadata_uri": None},
    )

    def failing_load(**kwargs):
        raise RuntimeError("BigQuery unavailable")

    monkeypatch.setattr(observations, "publish_to_bigquery", failing_load)

    class Client:
        def cuadro(self, series, nombres, desde, hasta):
            return pd.DataFrame(
                {"usd_clp": [900.0, 901.0]},
                index=pd.to_datetime(["2026-09-30", "2026-10-01"]),
            )

    counts = RowCounts()

    with pytest.raises(RuntimeError):
        observations.ingest_series(
            client=Client(),
            series_config={
                "name": "usd_clp",
                "code": "F073.TCO.PRE.Z.D",
                "start_date": "2026-09-30",
                "frequency": "daily",
                "quality": {"max_null_ratio": 0.5},
            },
            end_date="2026-10-01",
            run=bcch.IngestionRun("r", STARTED_AT, "bcch_ingestion", "observations"),
            counts=counts,
        )

    assert counts == RowCounts(extracted=2, valid=2, rejected=0, loaded=0)
