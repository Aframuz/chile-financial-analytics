import json
from datetime import datetime, timezone

import requests

from src.common.exit_codes import (
    EXIT_FAILURE,
    EXIT_SUCCESS,
    EXIT_TEMPFAIL,
)
from src.ingestion.bcch import (
    RunResult,
    save_run_summary,
    step_report,
)
from src.pipelines import bcch as pipeline


def make_result(tmp_path, name, rows_changed, exit_code=EXIT_SUCCESS):

    path = save_run_summary(
        results=[{"status": "success", "rows_changed": rows_changed}],
        run_id=f"{name}-run",
        started_at=datetime.now(timezone.utc),
        ended_at=datetime.now(timezone.utc),
        pipeline=name,
        runs_dir=tmp_path / name,
    )

    return RunResult(exit_code=exit_code, summary_path=path)


def test_summary_sums_rows_changed_unless_given(tmp_path):

    path = save_run_summary(
        results=[
            {"status": "success", "row_count": 10, "valid_rows": 10, "loaded_rows": 10, "rows_changed": 3},
            {"status": "quality_failed", "row_count": 5, "rejected_rows": 5, "rows_changed": 4,
             "failed_checks": ["min_value"]},
        ],
        run_id="test-run",
        started_at=datetime.now(timezone.utc),
        ended_at=datetime.now(timezone.utc),
        pipeline="test",
        runs_dir=tmp_path,
    )

    summary = json.loads(path.read_text())

    assert summary["run_id"] == "test-run"
    assert summary["rows_changed"] == 7
    # Rows through each stage
    assert summary["extracted_rows"] == 15
    assert summary["valid_rows"] == 10
    assert summary["rejected_rows"] == 5
    assert summary["loaded_rows"] == 10
    assert summary["error_message"] == "None: quality_failed (min_value)"

    report = step_report(RunResult(EXIT_SUCCESS, path))

    assert report["rows_changed"] == 7
    assert report["exit_code"] == EXIT_SUCCESS


def test_pipeline_report_combines_steps(tmp_path, monkeypatch):

    monkeypatch.setattr(
        pipeline.series,
        "run_series",
        lambda: make_result(tmp_path, "series", 1),
    )
    monkeypatch.setattr(
        pipeline.observations,
        "run_observations",
        lambda start_date, end_date: make_result(
            tmp_path, "observations", 5, EXIT_TEMPFAIL
        ),
    )

    result = pipeline.run_bcch_pipeline(end_date="2026-10-01")

    assert result.exit_code == EXIT_TEMPFAIL
    assert result.report["rows_changed"] == 6
    assert result.report["steps"]["observations"]["exit_code"] == EXIT_TEMPFAIL


def test_crashed_step_makes_rows_changed_unknown(tmp_path, monkeypatch):

    def crash():
        raise requests.ConnectionError()

    monkeypatch.setattr(pipeline.series, "run_series", crash)
    monkeypatch.setattr(
        pipeline.observations,
        "run_observations",
        lambda start_date, end_date: make_result(tmp_path, "observations", 0),
    )

    result = pipeline.run_bcch_pipeline()

    assert result.exit_code == EXIT_TEMPFAIL
    assert result.report["steps"]["series"]["status"] == "crashed"
    # Unknown, so the orchestrator must not assume nothing changed
    assert result.report["rows_changed"] is None


def test_main_prints_report_as_last_line(tmp_path, monkeypatch, capsys):

    monkeypatch.setattr(
        pipeline.series,
        "run_series",
        lambda: make_result(tmp_path, "series", 0),
    )
    monkeypatch.setattr(
        pipeline.observations,
        "run_observations",
        lambda start_date, end_date: make_result(
            tmp_path, "observations", 0, EXIT_FAILURE
        ),
    )

    exit_code = pipeline.main([])

    last_line = capsys.readouterr().out.strip().splitlines()[-1]

    assert exit_code == EXIT_FAILURE
    assert json.loads(last_line)["rows_changed"] == 0
