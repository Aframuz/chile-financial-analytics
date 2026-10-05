from datetime import datetime, timezone

import pandas as pd

from src.transforms.bcch import (
    prepare_bcch_observations,
)

def test_prepare_bcch_observations():

    df = pd.DataFrame(
        {
            "usd_clp": [
                900.0,
                901.0,
            ]
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
            ]
        ),
    )

    config = {
        "name": "usd_clp",
        "code": "TEST.CODE",
        "frequency": "daily",
        "unit": "clp_per_usd",
    }

    extracted_at = datetime(
        2026,
        9,
        26,
        15,
        30,
        tzinfo=timezone.utc,
    )

    ingested_at = datetime(
        2026,
        9,
        26,
        15,
        32,
        tzinfo=timezone.utc,
    )

    result = prepare_bcch_observations(
        df=df,
        series_config=config,
        extracted_at=extracted_at,
        ingested_at=ingested_at,
    )

    assert len(result) == 2

    assert list(result.columns) == [
        "observation_date",
        "series_code",
        "series_name",
        "value",
        "frequency",
        "unit",
        "extraction_date",
        "extracted_at",
        "ingested_at",
        "source",
    ]

    assert (
        result.iloc[0]["series_code"]
        == "TEST.CODE"
    )

    assert (
        result.iloc[0]["value"]
        == 900.0
    )

    assert (
        result.iloc[0]["source"]
        == "bcch"
    )

    assert (
        result["ingested_at"] == ingested_at
    ).all()
