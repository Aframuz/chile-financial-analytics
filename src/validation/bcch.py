from typing import Any

import pandas as pd


def validate_series_data(
    df: pd.DataFrame,
    series_config: dict[str, Any],
    start_date: str,
    end_date: str,
) -> dict[str, Any]:

    series_name = series_config["name"]

    quality_config = series_config.get(
        "quality",
        {},
    )

    checks: list[dict[str, Any]] = []

    def add_check(
        name: str,
        passed: bool,
        message: str,
    ) -> None:

        checks.append(
            {
                "name": name,
                "passed": passed,
                "message": message,
            }
        )

    # ----------------------------
    # 1. DataFrame must not be empty
    # ----------------------------

    add_check(
        name="not_empty",
        passed=not df.empty,
        message=f"row_count={len(df)}",
    )

    if df.empty:
        return {
            "passed": False,
            "checks": checks,
            "metrics": {
                "row_count": 0,
            },
        }

    # ----------------------------
    # 2. Expected column
    # ----------------------------

    expected_columns = {series_name}

    actual_columns = set(df.columns)

    expected_column_present = (
        series_name in actual_columns
    )

    add_check(
        name="expected_column_present",
        passed=expected_column_present,
        message=(
            f"expected={series_name}, "
            f"actual={list(df.columns)}"
        ),
    )

    unexpected_columns = (
        actual_columns - expected_columns
    )

    add_check(
        name="no_unexpected_columns",
        passed=not unexpected_columns,
        message=(
            "unexpected_columns="
            f"{sorted(unexpected_columns)}"
        ),
    )

    if not expected_column_present:
        return {
            "passed": False,
            "checks": checks,
            "metrics": {
                "row_count": len(df),
            },
        }

    # ----------------------------
    # 3. Validate dates
    # ----------------------------

    parsed_dates = pd.to_datetime(
        df.index,
        errors="coerce",
    )

    invalid_date_count = int(
        parsed_dates.isna().sum()
    )

    add_check(
        name="valid_dates",
        passed=invalid_date_count == 0,
        message=(
            f"invalid_date_count="
            f"{invalid_date_count}"
        ),
    )

    duplicate_count = int(
        pd.Index(parsed_dates)
        .duplicated()
        .sum()
    )

    allow_duplicates = quality_config.get(
        "allow_duplicate_dates",
        False,
    )

    add_check(
        name="duplicate_dates",
        passed=(
            allow_duplicates
            or duplicate_count == 0
        ),
        message=(
            f"duplicate_count="
            f"{duplicate_count}"
        ),
    )

    valid_dates = parsed_dates.dropna()

    if len(valid_dates) > 0:

        lower_bound = pd.Timestamp(
            start_date
        )

        upper_bound = pd.Timestamp(
            end_date
        )

        out_of_range_count = int(
            (
                (valid_dates < lower_bound)
                | (valid_dates > upper_bound)
            ).sum()
        )

    else:
        out_of_range_count = len(df)

    add_check(
        name="dates_within_requested_range",
        passed=out_of_range_count == 0,
        message=(
            f"out_of_range_count="
            f"{out_of_range_count}"
        ),
    )

    # ----------------------------
    # 4. Validate values
    # ----------------------------

    values = df[series_name]

    null_count = int(
        values.isna().sum()
    )

    null_ratio = (
        null_count / len(df)
        if len(df)
        else 0.0
    )

    max_null_ratio = quality_config.get(
        "max_null_ratio",
        1.0,
    )

    add_check(
        name="null_ratio",
        passed=(
            null_ratio
            <= max_null_ratio
        ),
        message=(
            f"null_ratio="
            f"{null_ratio:.4f}, "
            f"max_allowed="
            f"{max_null_ratio:.4f}"
        ),
    )

    numeric_values = pd.to_numeric(
        values,
        errors="coerce",
    )

    original_non_null = (
        values.notna()
    )

    became_null_after_conversion = (
        original_non_null
        & numeric_values.isna()
    )

    non_numeric_count = int(
        became_null_after_conversion.sum()
    )

    require_numeric = quality_config.get(
        "require_numeric",
        True,
    )

    add_check(
        name="numeric_values",
        passed=(
            not require_numeric
            or non_numeric_count == 0
        ),
        message=(
            f"non_numeric_count="
            f"{non_numeric_count}"
        ),
    )

    # ----------------------------
    # 5. Optional minimum
    # ----------------------------

    min_value = quality_config.get(
        "min_value"
    )

    below_minimum_count = 0

    if min_value is not None:

        below_minimum_count = int(
            (
                numeric_values.dropna()
                < min_value
            ).sum()
        )

        add_check(
            name="minimum_value",
            passed=(
                below_minimum_count == 0
            ),
            message=(
                f"below_minimum_count="
                f"{below_minimum_count}, "
                f"minimum={min_value}"
            ),
        )

    # ----------------------------
    # Final result
    # ----------------------------

    passed = all(
        check["passed"]
        for check in checks
    )

    metrics = {
        "row_count": len(df),
        "null_count": null_count,
        "null_ratio": null_ratio,
        "duplicate_date_count":
            duplicate_count,
        "invalid_date_count":
            invalid_date_count,
        "non_numeric_count":
            non_numeric_count,
        "below_minimum_count":
            below_minimum_count,
    }

    return {
        "passed": passed,
        "checks": checks,
        "metrics": metrics,
    }

CURATED_REQUIRED_FIELDS = (
    "series_name",
    "category",
    "unit",
)


def validate_series_metadata(
    series_config: dict[str, Any],
    catalog_matches: pd.DataFrame,
    curated: dict[str, Any] | None,
) -> dict[str, Any]:
    """Validate the metadata of one series.

    catalog_matches: rows of the BCCh SearchSeries catalog
    whose seriesId equals the configured code.

    curated: the entry of metadata/bcch/series.yml for the
    configured code, or None if missing.
    """

    checks: list[dict[str, Any]] = []

    def add_check(
        name: str,
        passed: bool,
        message: str,
    ) -> None:

        checks.append(
            {
                "name": name,
                "passed": passed,
                "message": message,
            }
        )

    match_count = len(catalog_matches)

    # ----------------------------
    # 1. Source catalog
    # ----------------------------

    add_check(
        name="found_in_catalog",
        passed=match_count > 0,
        message=f"match_count={match_count}",
    )

    # Grain: one row per series_code
    add_check(
        name="unique_in_catalog",
        passed=match_count <= 1,
        message=f"match_count={match_count}",
    )

    if match_count == 1:

        source_frequency = str(
            catalog_matches["frequencyCode"].iloc[0]
        ).lower()

        configured_frequency = series_config.get(
            "frequency"
        )

        add_check(
            name="frequency_matches_config",
            passed=(
                configured_frequency is None
                or configured_frequency.lower()
                == source_frequency
            ),
            message=(
                f"source={source_frequency}, "
                f"configured={configured_frequency}"
            ),
        )

    # ----------------------------
    # 2. Curated metadata
    # ----------------------------

    add_check(
        name="curated_metadata_present",
        passed=curated is not None,
        message=f"series_code={series_config['code']}",
    )

    missing_fields = [
        field
        for field in CURATED_REQUIRED_FIELDS
        if not (curated or {}).get(field)
    ]

    add_check(
        name="curated_required_fields",
        passed=not missing_fields,
        message=f"missing_fields={missing_fields}",
    )

    # ----------------------------
    # Final result
    # ----------------------------

    passed = all(
        check["passed"]
        for check in checks
    )

    return {
        "passed": passed,
        "checks": checks,
        "metrics": {
            "catalog_match_count": match_count,
        },
    }
