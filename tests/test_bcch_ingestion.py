import pandas as pd

from src.ingestion.observations import (
    extract_series,
)

class FakeBCChClient:

    def cuadro(
        self,
        series,
        nombres,
        desde,
        hasta,
    ):

        return pd.DataFrame(
            {
                nombres[0]: [
                    900.0,
                    901.0,
                ]
            },
            index=pd.to_datetime(
                [
                    desde,
                    hasta,
                ]
            ),
        )
        
def test_extract_series_uses_client():

    client = FakeBCChClient()

    df = extract_series(
        client=client,
        series_code="TEST.CODE",
        series_name="usd_clp",
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert len(df) == 2

    assert "usd_clp" in df.columns