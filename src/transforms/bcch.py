from datetime import datetime

import pandas as pd

def prepare_bcch_observations(
    df: pd.DataFrame,
    series_config: dict,
    extracted_at: datetime,
) -> pd.DataFrame:

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
            "source",
        ]
    ]