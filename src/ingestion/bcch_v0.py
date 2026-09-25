import os
from pathlib import Path

import bcchapi
import pandas as pd
from dotenv import load_dotenv

import json

from datetime import datetime, timezone
from importlib.metadata import version


SERIES_CODE = "F073.TCO.PRE.Z.D"
SERIES_NAME = "usd_clp"

START_DATE = "2024-01-01"
END_DATE = "2024-01-31"

RAW_DATA_DIR = Path("data/raw/bcch")


def create_client() -> bcchapi.Siete:
    load_dotenv()

    token = os.getenv("BCCH_API_TOKEN")

    if not token:
        raise RuntimeError(
            "BCCH_API_TOKEN environment variable is not configured."
        )

    return bcchapi.Siete(token=token)


def extract_series(
    client: bcchapi.Siete,
    series_code: str,
    series_name: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return client.cuadro(
        series=[series_code],
        nombres=[series_name],
        desde=start_date,
        hasta=end_date,
    )

def save_raw(
    df: pd.DataFrame,
    series_name: str,
) -> Path:
    output_dir = RAW_DATA_DIR / series_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "data.json"

    df.to_json(
        output_path,
        orient="table",
        date_format="iso",
        indent=2,
    )

    return output_path


def save_metadata(
    series_code: str,
    series_name: str,
    start_date: str,
    end_date: str,
    row_count: int,
    output_dir: Path,
) -> Path:
    metadata = {
        "source": "Banco Central de Chile - BDE",
        "series_code": series_code,
        "series_name": series_name,
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "extractor": "bcchapi",
        "bcchapi_version": version("bcchapi"),
    }

    metadata_path = output_dir / "metadata.json"

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return metadata_path

def main() -> None:
    client = create_client()

    df = extract_series(
        client=client,
        series_code=SERIES_CODE,
        series_name=SERIES_NAME,
        start_date=START_DATE,
        end_date=END_DATE,
    )

    if df.empty:
        raise RuntimeError(
            f"No data returned for series {SERIES_CODE}"
        )

    output_path = save_raw(
        df=df,
        series_name=SERIES_NAME,
    )

    save_metadata(
        series_code=SERIES_CODE,
        series_name=SERIES_NAME,
        start_date=START_DATE,
        end_date=END_DATE,
        row_count=len(df),
        output_dir=output_path.parent,
    )

    print(f"Saved {len(df)} observations to {output_path}")


if __name__ == "__main__":
    main()
