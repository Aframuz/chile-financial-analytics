import os
import sys
from pathlib import Path

# Before Airflow reads its config: example DAGs would be parsed too
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

# DAG modules import bcch_common the way the DAG processor sees it: from
# the dags folder on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
