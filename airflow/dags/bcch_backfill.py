"""
# BCCh backfill

Re-ingests BCCh **observations** for past periods, **one calendar month per
run**, into `raw_bcch.observations`. Use it to reprocess or extend history;
regular loads are done by `bcch_financial_pipeline`.

## How to run

From the DAG page, **Trigger → Backfill**, or with the CLI:

```bash
airflow backfill create --dag-id bcch_backfill \
    --from-date 2019-01-01 --to-date 2019-12-31
```

Each run ingests exactly its month (e.g. 2019-03-01 → 2019-03-31). A
monthly series has one value per chunk (dated the 1st); a daily series
has that month's days. Months run one at a time.

## Then

**Trigger `bcch_financial_pipeline`.** This DAG only loads raw data. The
next main run rebuilds the marts, even if its own ingestion changes
nothing: each chunk that changed rows sets the `bcch_marts_stale` Variable.

## Good to know

- **Keep this DAG unpaused.** Airflow only starts backfill runs on unpaused
  DAGs. The DAG's own monthly schedule exists only to define the chunks:
  those scheduled runs skip themselves.
- **Before a series' configured `start_date`** (2020-01-01), a backfill
  extends history, but regular runs won't refresh those older rows. To keep
  them current, move `start_date` back in `config/bcch_series.yml` instead.
- **Raw files** go to `…/extraction_date=<today>/window=<start>_<end>/`, so
  chunks extracted the same day don't overwrite each other.
- **Failures and retries** follow the same exit-code rules as
  `bcch_financial_pipeline` (transient errors retry, others fail at once),
  and the shared `bcch_bigquery` pool keeps backfill and regular tasks from
  touching BigQuery at the same time.

Code: `airflow/dags/bcch_backfill.py`, `src/ingestion/observations.py`.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.timetables.interval import CronDataIntervalTimetable

from bcch_common import (
    BIGQUERY_POOL,
    INGESTION_RETRYABLE_EXIT_CODES,
    mark_marts_stale,
    parse_report,
    raise_for_exit_codes,
    run_command,
)


@dag(
    dag_id="bcch_backfill",
    # Real [start, end) intervals: one calendar month per run.
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    # Earliest month a backfill may request.
    start_date=datetime(1990, 1, 1),
    catchup=False,
    # Backfill runs only start on an unpaused DAG; the timetable's own
    # monthly runs skip themselves (see ingest_window).
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(minutes=30),
        "pool": BIGQUERY_POOL,
    },
    tags=["bcch", "financial", "backfill"],
    description="Re-ingest BCCh observations one month per run; use with Airflow backfills.",
    doc_md=__doc__,
)
def bcch_backfill():

    @task
    def ingest_window(dag_run=None, data_interval_start=None, data_interval_end=None) -> dict:
        """
        Ingest observations for this run's month
        (`python -m src.ingestion.observations --start-date … --end-date …`).

        Skips itself on scheduled runs. A series with no observation in the
        month counts as "no data", not as a failure.

        **XCom**: run id, rows changed, duration, API retries, run summary URI.
        """
        if dag_run.run_type == "scheduled":
            # Regular ingestion is bcch_financial_pipeline (full history).
            raise AirflowSkipException(
                "bcch_backfill only processes backfill and manual runs."
            )

        if data_interval_start is None:
            # Manual triggers without a logical date have no interval.
            raise AirflowFailException(
                "No data interval: run this DAG through a backfill, or "
                "trigger it with a logical date inside the month to load."
            )

        # BCCh dates are inclusive; the interval end is exclusive.
        start_date = data_interval_start.date().isoformat()
        end_date = (data_interval_end - timedelta(days=1)).date().isoformat()

        result = run_command(
            "python -m src.ingestion.observations"
            f" --start-date {start_date} --end-date {end_date}"
        )
        report = parse_report(result.output) or {"rows_changed": None}

        mark_marts_stale(report["rows_changed"])

        raise_for_exit_codes([result.exit_code], INGESTION_RETRYABLE_EXIT_CODES)

        # XCom: run id, rows changed and summary path for this chunk.
        return report

    ingest_window()


bcch_backfill()
