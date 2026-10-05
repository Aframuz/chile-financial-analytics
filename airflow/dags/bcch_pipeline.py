"""
# BCCh financial pipeline

Loads Banco Central de Chile (BCCh) series into BigQuery and builds the
analytics marts with dbt.

```
ingest_series ──────┐
                    ├→ has_changes → snapshot → transform → quality ─┐
ingest_observations ┘                                                ├→ report
                    └→ freshness ────────────────────────────────────┘
```

| Task | What it does | Writes |
|---|---|---|
| `ingest_series` | Series metadata from the BCCh catalog + `metadata/bcch/series.yml` | `raw_bcch.series` |
| `ingest_observations` | Observations for the configured series (full history by default) | `raw_bcch.observations` |
| `has_changes` | Skips dbt when raw data hasn't changed since the last successful `transform` | – |
| `snapshot` | `dbt snapshot`: SCD2 history of series metadata | `analytics_dev_snapshots` |
| `transform` | `dbt run`: staging views, `dim_date`, `dim_series`, `fact_economic_observation` | `analytics_dev` |
| `quality` | `dbt test` | – |
| `freshness` | `dbt source freshness`: hours since new data landed for each series, against its cadence (daily or monthly). Reports, never fails the run | – |
| `report` | Collects every task's report, logs it and emits `bcch.*` metrics | – |

Raw files and run summaries also land in GCS (`gs://<raw bucket>/raw/bcch/…`
and `…/_runs/…`). Series to ingest: `config/bcch_series.yml`.

## Trigger parameters

| Parameter | Default | Use |
|---|---|---|
| `start_date` | empty = full history | First observation date to reload. Leave empty for regular runs: BCCh revises history and publishes monthly series ~2 months late, so only full history catches every change. |
| `end_date` | the run's date | Last observation date. |
| `force_dbt` | `false` | Run dbt even if no raw data changed, e.g. after deploying changed dbt models. |

## Common operations

- **Regular run**: trigger with defaults.
- **Deployed dbt changes**: trigger with `force_dbt = true`; otherwise a run with
  no new data skips dbt and the change isn't built.
- **Reload a period**: set `start_date`/`end_date`; for many months use the
  `bcch_backfill` DAG.
- **After a backfill**: trigger this DAG. Backfills only load raw data; the
  `bcch_marts_stale` Variable makes this run rebuild the marts.

## Failures and retries

Tasks run the project's CLI commands; the exit code decides:

| Exit code | Meaning | Airflow |
|---|---|---|
| `0` | success | continue |
| `75` (ingestion), `2` (dbt) | transient: network, timeouts, BCCh/BigQuery outages | retried 2× with backoff (5 → 30 min) |
| any other | permanent: invalid credentials or config, data-quality failure, SQL or test failure | fails at once, no retry |

The ingestion tasks are independent: one failing doesn't skip the other,
but dbt only runs when both succeed. Each task times out after 30 minutes.

`freshness` is the exception: a stale series (dbt exits 1) is reported,
not raised, so a late BCCh release doesn't fail the run. Alert on
`airflow_bcch_dbt_nodes_freshness_error` (stale series count) or
`airflow_bcch_hours_since_loaded_bcch_observations__<series>` instead.
Only a failing freshness query (e.g. a missing column) fails it. It runs
even if an ingestion task failed, to show how stale raw data is.

**Concurrency**: one run at a time (`max_active_runs=1`), and the
`bcch_bigquery` pool (1 slot, shared with `bcch_backfill`) allows only one
task touching BigQuery at a time.

## Where to look

- **Run history**: `monitoring.ingestion_runs` in BigQuery, one audit row
  per ingestion run (failures included): status, row counts per stage,
  error, Airflow run. Raw and mart rows carry `ingestion_run_id`.
- **Task XComs**: ingestion run id, rows changed, duration, API retries and
  the run summary's `gs://` URI; dbt status counts and `invocation_id`;
  `report` has everything.
- **Task logs**: each line carries `run=<ingestion run id>` and
  `airflow=<dag>/<run>/<task>#<try>`. Also stored in `gs://<raw bucket>/airflow-logs/`.
- **BigQuery jobs**: labelled with the Airflow run (`airflow_run_id`) and
  ingestion run id or dbt invocation id; query `INFORMATION_SCHEMA.JOBS`
  (see README).
- **Metrics**: `airflow_bcch_*` at http://localhost:9102/metrics.

Code: `airflow/dags/bcch_pipeline.py`, `src/ingestion/`, `dbt/chile_financial_analytics/`.
"""
import logging
import os
import shlex
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task
from airflow.sdk.observability.stats import Stats

from bcch_common import (
    BIGQUERY_POOL,
    INGESTION_RETRYABLE_EXIT_CODES,
    dbt_run_paths,
    dbt_summary,
    freshness_summary,
    mark_marts_fresh,
    mark_marts_stale,
    marts_stale,
    parse_report,
    raise_for_exit_codes,
    run_command,
)

log = logging.getLogger(__name__)

DBT = (
    '"$DBT_BIN" {command} --project-dir "$DBT_PROJECT_DIR"'
    " --target-path {target_path} --log-path {log_path}"
)

DBT_RETRYABLE_EXIT_CODES = {2}  # unhandled error, e.g. network interruption


def run_dbt(command: str) -> dict:
    """Run a dbt command; return its status counts (pushed to XCom)."""

    paths = dbt_run_paths()

    result = run_command(
        DBT.format(
            command=command,
            target_path=shlex.quote(paths["target_path"]),
            log_path=shlex.quote(paths["log_path"]),
        )
    )

    if result.exit_code == 0:
        return dbt_summary(command, paths["target_path"])

    raise_for_exit_codes([result.exit_code], DBT_RETRYABLE_EXIT_CODES)


def run_dbt_freshness() -> dict:
    """Run `dbt source freshness`; return its status counts, stale or not.

    dbt exits 1 both when a source is stale and when its freshness query
    fails; sources.json tells them apart (error vs runtime error).
    """

    paths = dbt_run_paths()

    result = run_command(
        DBT.format(
            command="source freshness",
            target_path=shlex.quote(paths["target_path"]),
            log_path=shlex.quote(paths["log_path"]),
        )
    )

    if result.exit_code in (0, 1) and os.path.exists(
        os.path.join(paths["target_path"], "sources.json")
    ):
        summary = freshness_summary(paths["target_path"])

        if "runtime error" not in summary["statuses"]:

            if result.exit_code == 1:
                log.warning(
                    "Stale sources (not failing the run): %s, hours since loaded: %s",
                    summary["statuses"],
                    summary["hours_since_loaded"],
                )

            return summary

    raise_for_exit_codes([result.exit_code], DBT_RETRYABLE_EXIT_CODES)


def run_ingestion(command: str) -> dict:
    """Run an ingestion entrypoint; return its JSON report (pushed to XCom)."""

    result = run_command(command)
    report = parse_report(result.output) or {"rows_changed": None}

    # Even a failed run may have loaded some series before failing.
    mark_marts_stale(report["rows_changed"])

    raise_for_exit_codes([result.exit_code], INGESTION_RETRYABLE_EXIT_CODES)

    return report


def emit_metrics(summary: dict) -> None:
    """StatsD metrics (exported to Prometheus format by statsd-exporter).

    Time series for trends logs can't show, e.g. rows_changed stuck
    at 0 for days or a step getting slower.
    """

    for task_id, ingest_report in summary["ingest"].items():

        if not ingest_report:
            continue

        if ingest_report.get("rows_changed") is not None:
            Stats.gauge(f"bcch.rows_changed.{task_id}", ingest_report["rows_changed"])

        if ingest_report.get("duration_seconds") is not None:
            Stats.gauge(f"bcch.duration_seconds.{task_id}", ingest_report["duration_seconds"])

        if ingest_report.get("api_retries") is not None:
            Stats.gauge(f"bcch.api_retries.{task_id}", ingest_report["api_retries"])

    Stats.gauge("bcch.dbt_ran", int(summary["dbt_ran"]))

    for task_id, dbt_report in summary["dbt"].items():

        if not dbt_report:
            continue

        Stats.gauge(f"bcch.duration_seconds.{task_id}", dbt_report["elapsed_seconds"])

        statuses = dbt_report["statuses"]

        # A gauge keeps its last value: without the zeros, a stale
        # source that recovers would keep reporting freshness.error.
        if task_id == "freshness":
            statuses = {"pass": 0, "warn": 0, "error": 0, **statuses}

        for status, count in statuses.items():
            Stats.gauge(f"bcch.dbt_nodes.{task_id}.{status}", count)

        for source, hours in dbt_report.get("hours_since_loaded", {}).items():
            Stats.gauge(f"bcch.hours_since_loaded.{source}", hours)


@dag(
    dag_id="bcch_financial_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    # Concurrent runs would MERGE the same raw tables and build the same
    # dbt models at once; BigQuery rejects conflicting concurrent DML.
    max_active_runs=1,
    # Optional window for historical reloads, set in the trigger form.
    # Empty start_date = full history (catches revisions and monthly
    # series published ~2 months late); empty end_date = run's date.
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="First observation date (YYYY-MM-DD). Empty = full history.",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Last observation date (YYYY-MM-DD). Empty = the run's data interval end.",
        ),
        "force_dbt": Param(
            False,
            type="boolean",
            description=(
                "Run dbt even if raw data didn't change, e.g. after "
                "deploying changed dbt models."
            ),
        ),
    },
    default_args={
        # Second line of defence: src/ already retries transient BCCh
        # errors within seconds; these cover longer outages. Only
        # retryable exit codes use them (see raise_for_exit_codes).
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        # bcchapi issues requests without a timeout; a hung call must
        # still fail (and retry) instead of blocking the run forever.
        "execution_timeout": timedelta(minutes=30),
        # Shared with bcch_backfill: one BigQuery-touching task at a time.
        "pool": BIGQUERY_POOL,
    },
    tags=["bcch", "financial"],
    description="BCCh series → raw_bcch (BigQuery) → dbt snapshot, models and tests.",
    doc_md=__doc__,
)
def bcch_financial_pipeline():

    @task
    def ingest_series() -> dict:
        """
        Load **series metadata** into `raw_bcch.series`
        (`python -m src.ingestion.series`).

        Merges the BCCh catalog (frequency, coverage, titles) with the curated
        names, categories and units in `metadata/bcch/series.yml`. A catalog
        snapshot: no date window. Series removed from `config/bcch_series.yml`
        are deleted.

        **XCom**: run id, rows changed, duration, API retries, run summary URI.
        """
        return run_ingestion("python -m src.ingestion.series")

    @task
    def ingest_observations(params=None, data_interval_end=None, dag_run=None) -> dict:
        """
        Load **observations** into `raw_bcch.observations`
        (`python -m src.ingestion.observations`).

        Extracts each configured series from its `start_date` (or the
        `start_date` param) to `end_date` (default: the run's date), validates
        it, writes raw files to GCS and MERGEs into BigQuery. Only rows whose
        values changed are updated.

        **XCom**: run id, rows changed, duration, API retries, run summary URI.
        """
        start_date = params.get("start_date")
        # Pin the window end to the run, not "now", so reruns are reproducible.
        # Runs triggered without a logical date have no data interval;
        # run_after (when the run was due) is always set.
        run_end = data_interval_end or dag_run.run_after
        end_date = params.get("end_date") or run_end.date().isoformat()

        command = f"python -m src.ingestion.observations --end-date {shlex.quote(end_date)}"

        if start_date:
            command += f" --start-date {shlex.quote(start_date)}"

        return run_ingestion(command)

    # Skips only its direct downstream (snapshot); transform and quality are
    # then skipped by their trigger rules, while report still runs.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_changes(series_report: dict, observations_report: dict, params=None) -> bool:
        """
        **Decide whether dbt runs.** dbt runs when any of these hold:

        - either ingestion task changed rows (or couldn't report how many),
        - the `bcch_marts_stale` Variable is set (a backfill, or an earlier run
          whose dbt failed, left raw changes not yet in the marts),
        - the `force_dbt` param is on.

        Otherwise `snapshot`, `transform` and `quality` are skipped; skipping is
        lossless since they would change nothing. `report` still runs.
        """
        step_rows = [
            series_report["rows_changed"],
            observations_report["rows_changed"],
        ]
        # Unknown if either step couldn't report.
        rows_changed = None if None in step_rows else sum(step_rows)
        stale = marts_stale()

        log.info(
            "rows_changed=%s (series=%s, observations=%s), marts_stale=%s, force_dbt=%s",
            rows_changed,
            *step_rows,
            stale,
            params["force_dbt"],
        )

        # Skipping is lossless: with no raw change, the incremental fact
        # model and the snapshot would be no-ops. Code changes are the
        # exception, hence force_dbt.
        return (
            params["force_dbt"]
            or rows_changed is None
            or rows_changed > 0
            or stale
        )

    @task
    def snapshot() -> dict:
        """
        `dbt snapshot`: record changes to series metadata (name, frequency,
        unit, category) as SCD Type 2 history in `bcch_series_snapshot`.

        **XCom**: dbt status counts, elapsed time, `invocation_id`.
        """
        return run_dbt("snapshot")

    @task
    def transform() -> dict:
        """
        `dbt run`: build staging views, `dim_date`, `dim_series` and the
        incremental `fact_economic_observation`, which picks up every raw row
        changed since its last build. On success, clears `bcch_marts_stale`.

        **XCom**: dbt status counts, elapsed time, `invocation_id`.
        """
        summary = run_dbt("run")
        # Marts now reflect raw_bcch (snapshot ran just before).
        mark_marts_fresh()
        return summary

    @task
    def quality() -> dict:
        """
        `dbt test`: source, staging and mart tests. A failing test fails the
        task without retries (it would fail again); only dbt infrastructure
        errors retry.

        **XCom**: test status counts (e.g. `{"pass": 66}`), `invocation_id`.
        """
        # Failing tests exit 1 (permanent); only infra errors (2) retry.
        return run_dbt("test")

    # After both ingestion tasks, even failed ones; independent of
    # has_changes, so it also runs when dbt is skipped (no new data is
    # exactly when freshness matters).
    @task(trigger_rule="all_done")
    def freshness() -> dict:
        """
        `dbt source freshness`: hours since new data last landed for each
        series in `raw_bcch.observations` (`ingested_at`), against its
        cadence's thresholds (daily or monthly) in
        `models/staging/bcch/_bcch_sources.yml`.

        A stale source is logged as a warning and reported, not raised:
        status `error` means stale past `error_after`. Only a failing
        freshness query fails the task.

        **XCom**: status counts (e.g. `{"warn": 1}`), hours since loaded,
        `invocation_id`.
        """
        return run_dbt_freshness()

    # Runs whether dbt ran or was short-circuited, but not after a failure.
    @task(trigger_rule="none_failed")
    def report(ti=None) -> dict:
        """
        Collect every task's XCom into one run summary: rows changed per
        ingestion task, whether dbt ran, test results, durations and run ids.
        Logs it and emits the `bcch.*` StatsD metrics.

        Runs whether dbt ran or was skipped, but not after a failure.
        """
        # Pulled by task id instead of passed as arguments: skipped dbt
        # tasks have no XCom, which xcom_pull returns as None.
        summary = {
            "ingest": {
                task_id: ti.xcom_pull(task_ids=task_id)
                for task_id in ("ingest_series", "ingest_observations")
            },
            "dbt": {
                task_id: ti.xcom_pull(task_ids=task_id)
                for task_id in ("snapshot", "transform", "quality", "freshness")
            },
        }

        summary["dbt_ran"] = summary["dbt"]["transform"] is not None

        log.info(
            "Run summary: rows_changed=%s, dbt_ran=%s, tests=%s, "
            "freshness=%s, durations=%s, ingestion run ids=%s",
            {
                task_id: ingest_report["rows_changed"]
                for task_id, ingest_report in summary["ingest"].items()
            },
            summary["dbt_ran"],
            (summary["dbt"]["quality"] or {}).get("statuses"),
            (summary["dbt"]["freshness"] or {}).get("hours_since_loaded"),
            {
                task_id: task_report.get("duration_seconds")
                or task_report.get("elapsed_seconds")
                for task_id, task_report in {
                    **summary["ingest"],
                    **summary["dbt"],
                }.items()
                if task_report
            },
            {
                task_id: ingest_report.get("run_id")
                for task_id, ingest_report in summary["ingest"].items()
            },
        )

        emit_metrics(summary)

        return summary

    series_report = ingest_series()
    observations_report = ingest_observations()
    run_report = report()

    (
        has_changes(series_report, observations_report)
        >> snapshot()
        >> transform()
        >> quality()
        >> run_report
    )

    [series_report, observations_report] >> freshness() >> run_report


bcch_financial_pipeline()
