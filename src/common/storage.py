import hashlib
import json

from pathlib import Path
from typing import Any

import pandas as pd

def calculate_sha256(
    path: Path,
) -> str:

    sha256 = hashlib.sha256()

    with path.open("rb") as file:

        while chunk := file.read(8192):
            sha256.update(chunk)

    return sha256.hexdigest()

def write_dataframe_json_atomic(
    df: pd.DataFrame,
    path: Path,
) -> str:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
        df.to_json(
            temp_path,
            orient="table",
            date_format="iso",
            indent=2,
        )

        temp_path.replace(path)

    finally:
        if temp_path.exists():
            temp_path.unlink()

    return calculate_sha256(path)

def write_json_atomic(
    data: dict[str, Any],
    path: Path,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
        temp_path.write_text(
            json.dumps(
                data,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        temp_path.replace(path)

    finally:
        if temp_path.exists():
            temp_path.unlink()