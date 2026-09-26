import pandas as pd

from src.validation.bcch import (
    validate_series_data,
)

def create_config() -> dict:

    return {
        "name": "test_series",

        "quality": {
            "max_null_ratio": 0.25,
            "require_numeric": True,
            "allow_duplicate_dates": False,
            "min_value": 0,
        },
    }
    
def test_valid_data_passes():

    df = pd.DataFrame(
        {
            "test_series": [
                100.0,
                101.0,
                102.0,
            ]
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
            ]
        ),
    )

    result = validate_series_data(
        df=df,
        series_config=create_config(),
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert result["passed"] is True
    
def test_duplicate_dates_fail():

    df = pd.DataFrame(
        {
            "test_series": [
                100.0,
                101.0,
            ]
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-01",
            ]
        ),
    )

    result = validate_series_data(
        df=df,
        series_config=create_config(),
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert result["passed"] is False

    assert (
        result["metrics"]
        ["duplicate_date_count"]
        == 1
    )
    
def test_excessive_null_ratio_fails():

    df = pd.DataFrame(
        {
            "test_series": [
                100.0,
                None,
                None,
                103.0,
            ]
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
            ]
        ),
    )

    result = validate_series_data(
        df=df,
        series_config=create_config(),
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert result["passed"] is False

    assert (
        result["metrics"]["null_ratio"]
        == 0.5
    )
    
def test_non_numeric_value_fails():

    df = pd.DataFrame(
        {
            "test_series": [
                100,
                "invalid",
                102,
            ]
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
            ]
        ),
    )

    result = validate_series_data(
        df=df,
        series_config=create_config(),
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert result["passed"] is False

    assert (
        result["metrics"]
        ["non_numeric_count"]
        == 1
    )
    
def test_value_below_minimum_fails():

    df = pd.DataFrame(
        {
            "test_series": [
                100,
                -1,
                102,
            ]
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
            ]
        ),
    )

    result = validate_series_data(
        df=df,
        series_config=create_config(),
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert result["passed"] is False

    assert (
        result["metrics"]
        ["below_minimum_count"]
        == 1
    )