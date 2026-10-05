from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from google.cloud import bigquery

from src.common import bigquery as bq
from src.common.bigquery import (
    OBSERVATIONS_KEY,
    OBSERVATIONS_SCHEMA,
    SERIES_KEY,
    SERIES_SCHEMA,
    add_missing_columns,
    build_merge_query,
    ensure_observations_table,
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


def test_observations_merge_ignores_ingested_at_changes():

    query = build_merge_query(
        target_table="p.raw_bcch.observations",
        staging_table="p.raw_bcch._staging_x",
        schema=OBSERVATIONS_SCHEMA,
        key_columns=OBSERVATIONS_KEY,
    )

    # A rerun with unchanged values must not count as a change...
    assert "target.ingested_at IS DISTINCT FROM" not in query
    # ...but a real change records when it was loaded
    assert "ingested_at = source.ingested_at" in query


class FakeClient:
    """Records schema updates and queries instead of calling BigQuery."""

    def __init__(self, table):
        self.table = table
        self.updated = []
        self.queries = []

    def get_table(self, table_id):
        return self.table

    def update_table(self, table, fields):
        self.updated.append(fields)
        return table

    def query(self, query, job_config=None):
        self.queries.append(query)
        return SimpleNamespace(
            result=lambda: None,
            num_dml_affected_rows=3,
            job_id="job",
        )


def _observations_table(schema):

    return bigquery.Table(
        "p.raw_bcch.observations",
        schema=schema,
    )


def test_add_missing_columns_appends_new_fields():

    old_schema = [
        field
        for field in OBSERVATIONS_SCHEMA
        if field.name != "ingested_at"
    ]
    table = _observations_table(old_schema)
    client = FakeClient(table)

    added = add_missing_columns(client, table, OBSERVATIONS_SCHEMA)

    assert added == ["ingested_at"]
    assert [field.name for field in table.schema][-1] == "ingested_at"
    assert client.updated == [["schema"]]


def test_add_missing_columns_is_noop_when_up_to_date():

    table = _observations_table(OBSERVATIONS_SCHEMA)
    client = FakeClient(table)

    assert add_missing_columns(client, table, OBSERVATIONS_SCHEMA) == []
    assert client.updated == []


def test_ensure_observations_table_backfills_new_ingested_at(monkeypatch):

    monkeypatch.setattr(bq, "job_labels", lambda: {})

    old_schema = [
        field
        for field in OBSERVATIONS_SCHEMA
        if field.name != "ingested_at"
    ]
    client = FakeClient(_observations_table(old_schema))

    ensure_observations_table(client, "p", "raw_bcch")

    assert len(client.queries) == 1
    assert "SET ingested_at = extracted_at" in client.queries[0]
    assert "WHERE ingested_at IS NULL" in client.queries[0]


def test_ensure_observations_table_skips_backfill_when_column_exists():

    client = FakeClient(_observations_table(OBSERVATIONS_SCHEMA))

    ensure_observations_table(client, "p", "raw_bcch")

    assert client.queries == []
    assert client.updated == []


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
