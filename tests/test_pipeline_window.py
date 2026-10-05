from datetime import datetime, timezone

import pandas as pd
import pytest

from src.common.exit_codes import (
    EXIT_FAILURE,
    EXIT_SUCCESS,
    EXIT_TEMPFAIL,
    combine_exit_codes,
)
from src.ingestion import observations
from src.ingestion.bcch import IngestionRun
from src.ingestion.observations import (
    ingest_series,
    iso_date,
    run_observations,
)


class EmptyBCChClient:

    def cuadro(self, series, nombres, desde, hasta):
        return pd.DataFrame()


SERIES_CONFIG = {
    "name": "imacec",
    "code": "F032.IMC.IND.Z.Z.EP18.Z.Z.0.M",
    "start_date": "2026-09-30",
    "frequency": "monthly",
}


@pytest.mark.parametrize(
    ("exit_codes", "expected"),
    [
        ([0, 0], EXIT_SUCCESS),
        ([0, 75], EXIT_TEMPFAIL),
        ([75, 75], EXIT_TEMPFAIL),
        ([75, 1], EXIT_FAILURE),
        ([2, 0], EXIT_FAILURE),
    ],
)
def test_combine_exit_codes(exit_codes, expected):

    assert combine_exit_codes(exit_codes) == expected


def test_empty_window_is_no_data_when_allowed(tmp_path, monkeypatch):

    # Nothing may be written when there is no data
    monkeypatch.setattr(observations, "RAW_DATA_DIR", tmp_path)

    result = ingest_series(
        client=EmptyBCChClient(),
        series_config=SERIES_CONFIG,
        end_date="2026-10-01",
        run=IngestionRun(
            run_id="test-run",
            started_at=datetime.now(timezone.utc),
            pipeline=observations.PIPELINE,
            table_name="observations",
        ),
        window="2026-09-30_2026-10-01",
    )

    assert result["status"] == "success"
    assert result["row_count"] == 0
    assert result["loaded_rows"] == 0
    assert not any(tmp_path.iterdir())


def test_iso_date_rejects_invalid_dates():

    assert iso_date("2026-10-01") == "2026-10-01"

    with pytest.raises(ValueError):
        iso_date("2026-13-01")


def test_run_observations_rejects_inverted_window(monkeypatch):

    monkeypatch.setattr(observations, "create_client", lambda: None)

    with pytest.raises(ValueError, match="after end_date"):
        run_observations(
            start_date="2026-10-02",
            end_date="2026-10-01",
        )


def test_explicit_window_gets_its_own_raw_folder():

    full = observations.build_output_dir(
        series_name="usd_clp",
        extraction_date="2026-10-01",
    )

    windowed = observations.build_output_dir(
        series_name="usd_clp",
        extraction_date="2026-10-01",
        window="2019-01-01_2019-01-31",
    )

    assert full.name == "extraction_date=2026-10-01"
    assert windowed.parent == full
    assert windowed.name == "window=2019-01-01_2019-01-31"
