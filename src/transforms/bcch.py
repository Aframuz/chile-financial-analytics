from datetime import datetime

import pandas as pd

def prepare_bcch_observations(
    df: pd.DataFrame,
    series_config: dict,
    extracted_at: datetime,
    ingested_at: datetime,
    ingestion_run_id: str,
) -> pd.DataFrame:
    """Canonical raw_bcch.observations rows.

    Grain: one row = one value of one series on one date.

    extracted_at: when the run fetched the data from BCCh.
    ingested_at: when the rows are loaded into the warehouse; drives
    dbt source freshness.
    ingestion_run_id: the run loading them (raw_bcch.ingestion_runs).
    """

    series_name = series_config["name"]

    result = df.copy()

    result = result.reset_index()

    result.columns = [
        "observation_date",
        "value",
    ]

    result["observation_date"] = (
        pd.to_datetime(
            result["observation_date"]
        ).dt.date
    )

    result["series_code"] = (
        series_config["code"]
    )

    result["series_name"] = (
        series_name
    )

    result["frequency"] = (
        series_config.get("frequency")
    )

    result["unit"] = (
        series_config.get("unit")
    )

    result["extraction_date"] = (
        extracted_at.date()
    )

    result["extracted_at"] = (
        extracted_at
    )

    result["ingested_at"] = (
        ingested_at
    )

    result["ingestion_run_id"] = (
        ingestion_run_id
    )

    result["source"] = "bcch"

    return result[
        [
            "observation_date",
            "series_code",
            "series_name",
            "value",
            "frequency",
            "unit",
            "extraction_date",
            "extracted_at",
            "ingested_at",
            "ingestion_run_id",
            "source",
        ]
    ]

def _to_date(values: pd.Series) -> pd.Series:

    parsed = pd.to_datetime(
        values,
        errors="coerce",
    )

    return (
        parsed.dt.date
        .astype(object)
        .where(parsed.notna(), None)
    )


def prepare_bcch_series(
    catalog: pd.DataFrame,
    curated: dict[str, dict],
    extracted_at: datetime,
    ingestion_run_id: str,
) -> pd.DataFrame:
    """Canonical raw_bcch.series rows.

    Grain: one row = latest known metadata
    for one BCCh series.

    catalog: BCCh SearchSeries rows, one per series.
    curated: metadata/bcch/series.yml entries keyed by code.
    ingestion_run_id: the run loading them (raw_bcch.ingestion_runs).
    """

    duplicated = catalog["seriesId"].duplicated()

    if duplicated.any():
        raise ValueError(
            "Duplicate series codes in catalog: "
            f"{sorted(catalog.loc[duplicated, 'seriesId'])}"
        )

    result = pd.DataFrame(
        {
            "series_code": catalog["seriesId"],

            "frequency": (
                catalog["frequencyCode"]
                .str.lower()
            ),

            "source_title_es": catalog["spanishTitle"],

            "source_title_en": catalog["englishTitle"],

            "first_observation_date": _to_date(
                catalog["firstObservation"]
            ),

            "last_observation_date": _to_date(
                catalog["lastObservation"]
            ),

            "source_updated_date": _to_date(
                catalog["updatedAt"]
            ),
        }
    ).reset_index(drop=True)

    for field in (
        "series_name",
        "description",
        "category",
        "unit",
    ):

        result[field] = [
            curated[code].get(field)
            for code in result["series_code"]
        ]

    result["extraction_date"] = (
        extracted_at.date()
    )

    result["extracted_at"] = (
        extracted_at
    )

    result["source"] = "bcch"

    result["ingestion_run_id"] = ingestion_run_id

    return result[
        [
            "series_code",
            "series_name",
            "description",
            "category",
            "frequency",
            "unit",
            "source_title_es",
            "source_title_en",
            "first_observation_date",
            "last_observation_date",
            "source_updated_date",
            "extraction_date",
            "extracted_at",
            "source",
            "ingestion_run_id",
        ]
    ]
