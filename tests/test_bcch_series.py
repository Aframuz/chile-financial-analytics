from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from src.common.bigquery import (
    OBSERVATIONS_KEY,
    OBSERVATIONS_SCHEMA,
    SERIES_KEY,
    SERIES_SCHEMA,
    build_merge_query,
)
from src.ingestion.series import (
    extract_catalog,
    load_curated_metadata,
)
from src.transforms.bcch import (
    prepare_bcch_series,
)
from src.validation.bcch import (
    validate_series_metadata,
)


EXTRACTED_AT = datetime(
    2026, 9, 28, 12, 0,
    tzinfo=timezone.utc,
)

SERIES_CONFIG = {
    "name": "usd_clp",
    "code": "F073.TCO.PRE.Z.D",
    "frequency": "daily",
}

CURATED = {
    "F073.TCO.PRE.Z.D": {
        "code": "F073.TCO.PRE.Z.D",
        "series_name": "USD/CLP",
        "description": "Tipo de cambio nominal (dólar observado)",
        "category": "Exchange Rate",
        "unit": "CLP per USD",
    },
}


def catalog_row(
    series_id="F073.TCO.PRE.Z.D",
    frequency="DAILY",
):

    return {
        "seriesId": series_id,
        "frequencyCode": frequency,
        "spanishTitle": "Tipo de cambio nominal",
        "englishTitle": "Nominal exchange rate",
        "firstObservation": pd.Timestamp("1982-08-09"),
        "lastObservation": pd.Timestamp("2026-09-25"),
        "updatedAt": pd.Timestamp("2026-09-26"),
        "createdAt": pd.Timestamp("2026-09-26"),
    }


def failed_checks(validation):

    return {
        check["name"]
        for check in validation["checks"]
        if not check["passed"]
    }


# ----------------------------
# Validation
# ----------------------------

def test_valid_metadata_passes():

    validation = validate_series_metadata(
        series_config=SERIES_CONFIG,
        catalog_matches=pd.DataFrame([catalog_row()]),
        curated=CURATED["F073.TCO.PRE.Z.D"],
    )

    assert validation["passed"]


def test_series_missing_from_catalog_fails():

    validation = validate_series_metadata(
        series_config=SERIES_CONFIG,
        catalog_matches=pd.DataFrame(
            columns=["seriesId", "frequencyCode"]
        ),
        curated=CURATED["F073.TCO.PRE.Z.D"],
    )

    assert failed_checks(validation) == {"found_in_catalog"}


def test_duplicate_catalog_rows_fail():

    validation = validate_series_metadata(
        series_config=SERIES_CONFIG,
        catalog_matches=pd.DataFrame(
            [catalog_row(), catalog_row()]
        ),
        curated=CURATED["F073.TCO.PRE.Z.D"],
    )

    assert failed_checks(validation) == {"unique_in_catalog"}


def test_frequency_mismatch_fails():

    validation = validate_series_metadata(
        series_config=SERIES_CONFIG,
        catalog_matches=pd.DataFrame(
            [catalog_row(frequency="MONTHLY")]
        ),
        curated=CURATED["F073.TCO.PRE.Z.D"],
    )

    assert failed_checks(validation) == {"frequency_matches_config"}


def test_missing_curated_metadata_fails():

    validation = validate_series_metadata(
        series_config=SERIES_CONFIG,
        catalog_matches=pd.DataFrame([catalog_row()]),
        curated=None,
    )

    assert failed_checks(validation) == {
        "curated_metadata_present",
        "curated_required_fields",
    }


def test_incomplete_curated_metadata_fails():

    validation = validate_series_metadata(
        series_config=SERIES_CONFIG,
        catalog_matches=pd.DataFrame([catalog_row()]),
        curated={"series_name": "USD/CLP"},
    )

    assert failed_checks(validation) == {"curated_required_fields"}


# ----------------------------
# Transform
# ----------------------------

def test_prepare_series_merges_catalog_and_curated():

    result = prepare_bcch_series(
        catalog=pd.DataFrame([catalog_row()]),
        curated=CURATED,
        extracted_at=EXTRACTED_AT,
    )

    assert list(result.columns) == [
        field.name
        for field in SERIES_SCHEMA
    ]

    row = result.iloc[0]

    assert row["series_code"] == "F073.TCO.PRE.Z.D"
    assert row["series_name"] == "USD/CLP"
    assert row["category"] == "Exchange Rate"
    assert row["frequency"] == "daily"
    assert row["unit"] == "CLP per USD"
    assert str(row["first_observation_date"]) == "1982-08-09"
    assert str(row["extraction_date"]) == "2026-09-28"
    assert row["source"] == "bcch"


def test_prepare_series_rejects_duplicate_codes():

    with pytest.raises(ValueError):

        prepare_bcch_series(
            catalog=pd.DataFrame(
                [catalog_row(), catalog_row()]
            ),
            curated=CURATED,
            extracted_at=EXTRACTED_AT,
        )


# ----------------------------
# Extraction
# ----------------------------

def test_extract_catalog_searches_every_frequency():

    searched = []

    def search(frequency):

        searched.append(frequency)

        return SimpleNamespace(
            to_df=lambda: pd.DataFrame(
                [catalog_row(f"CODE.{frequency}", frequency)]
            )
        )

    client = SimpleNamespace(
        stat=SimpleNamespace(
            session=SimpleNamespace(
                FREQUENCIES={"DAILY", "MONTHLY"},
                search=search,
            )
        )
    )

    catalog = extract_catalog(client)

    assert searched == ["DAILY", "MONTHLY"]
    assert list(catalog["seriesId"]) == [
        "CODE.DAILY",
        "CODE.MONTHLY",
    ]


def test_curated_metadata_rejects_duplicate_codes(tmp_path):

    path = tmp_path / "series.yml"

    path.write_text(
        "series:\n"
        "  - code: A\n"
        "  - code: A\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_curated_metadata(path)


# ----------------------------
# BigQuery
# ----------------------------

def test_series_merge_uses_series_code_as_key():

    query = build_merge_query(
        target_table="p.raw_bcch.series",
        staging_table="p.raw_bcch._staging_x",
        schema=SERIES_SCHEMA,
        key_columns=SERIES_KEY,
    )

    assert "target.series_code = source.series_code" in query
    assert "series_code = source.series_code," not in query
    assert "category = source.category" in query


def test_merge_only_updates_changed_rows():

    query = build_merge_query(
        target_table="p.raw_bcch.series",
        staging_table="p.raw_bcch._staging_x",
        schema=SERIES_SCHEMA,
        key_columns=SERIES_KEY,
    )

    assert (
        "target.category IS DISTINCT FROM source.category"
        in query
    )
    # Audit columns don't count as a change...
    assert "target.extracted_at IS DISTINCT FROM" not in query
    assert "target.extraction_date IS DISTINCT FROM" not in query
    # ...but are refreshed when something else changed
    assert "extracted_at = source.extracted_at" in query


def test_merge_without_delete_keeps_unmatched_rows():

    query = build_merge_query(
        target_table="p.raw_bcch.series",
        staging_table="p.raw_bcch._staging_x",
        schema=SERIES_SCHEMA,
        key_columns=SERIES_KEY,
    )

    assert "NOT MATCHED BY SOURCE" not in query


def test_merge_deletes_unmatched_except_retained_keys():

    query = build_merge_query(
        target_table="p.raw_bcch.series",
        staging_table="p.raw_bcch._staging_x",
        schema=SERIES_SCHEMA,
        key_columns=SERIES_KEY,
        delete_unmatched=True,
    )

    assert "WHEN NOT MATCHED BY SOURCE" in query
    assert (
        "target.series_code NOT IN UNNEST(@retained_keys)"
        in query
    )


def test_merge_delete_requires_single_key():

    with pytest.raises(ValueError):
        build_merge_query(
            target_table="p.raw_bcch.observations",
            staging_table="p.raw_bcch._staging_x",
            schema=OBSERVATIONS_SCHEMA,
            key_columns=OBSERVATIONS_KEY,
            delete_unmatched=True,
        )
