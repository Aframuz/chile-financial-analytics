import pandas as pd

from src.common.storage import (
    calculate_sha256,
    write_dataframe_json_atomic,
)

def test_dataframe_is_written_with_checksum(
    tmp_path,
):

    df = pd.DataFrame(
        {
            "value": [
                10,
                20,
                30,
            ]
        }
    )

    output_path = (
        tmp_path / "data.json"
    )

    checksum = (
        write_dataframe_json_atomic(
            df=df,
            path=output_path,
        )
    )

    assert output_path.exists()

    assert checksum == calculate_sha256(
        output_path
    )

    assert not (
        tmp_path / "data.json.tmp"
    ).exists()