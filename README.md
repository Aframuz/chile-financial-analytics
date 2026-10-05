## Architecture

### Raw ingestion

Banco Central BDE series are extracted using `bcchapi`,
validated against configurable data-quality expectations,
written as source-aligned JSON artifacts, and published to
Google Cloud Storage.

Raw objects follow:

raw/bcch/{series_name}/extraction_date={YYYY-MM-DD}/

Each partition contains:

- data.json
- metadata.json

Metadata includes extraction information, source series,
row counts, quality results, and SHA-256 checksum.

### Storage backends

The ingestion pipeline supports:

- `local`
- `gcs`

configured using the `STORAGE_BACKEND` environment variable.

### BigQuery raw model

`raw_bcch.observations`

**Grain:** one observation for one Banco Central series
on one observation date.

Natural key:

- `series_code`
- `observation_date`

The table is partitioned by `observation_date` and
clustered by `series_code`.

Loads use a staging table followed by a BigQuery `MERGE`
to support idempotent reruns and upstream revisions.

`raw_bcch.series`

**Grain:** latest known metadata for one Banco Central series.

Natural key:

- `series_code` (unique)

Built by merging two sources:

- the BCCh `SearchSeries` catalog (frequency, coverage dates, source titles)
- curated metadata in `metadata/bcch/series.yml` (name, category, unit)

`config/bcch_series.yml` decides *which* series are ingested;
`metadata/bcch/series.yml` describes *what* they are.

Raw artifacts are written to:

raw/bcch/_series/extraction_date={YYYY-MM-DD}/

### Running

```bash
python -m src.ingestion.observations   # raw_bcch.observations
python -m src.ingestion.series         # raw_bcch.series
```

```bash
python -m src.pipelines.bcch                       # series + observations, full history
python -m src.pipelines.bcch --start-date 2019-01-01 --end-date 2019-12-31
```

### Backfills

Regular runs (`bcch_financial_pipeline`) re-extract full history, which also
picks up BCCh revisions and monthly series published ~2 months late.

Historical processing uses the `bcch_backfill` DAG: one run per calendar
month, each ingesting exactly its window into `raw_bcch.observations`.

```bash
airflow backfill create --dag-id bcch_backfill \
    --from-date 2019-01-01 --to-date 2019-12-31
```

Or use the Backfill button on the DAG in the Airflow UI. Raw files for
explicit windows land in `.../extraction_date={YYYY-MM-DD}/window={start}_{end}/`.
The next `bcch_financial_pipeline` run loads the backfilled rows into the
marts (the fact model is incremental on `extracted_at`, not observation date).
Both DAGs share the `bcch_bigquery` pool (1 slot), so backfill and regular
tasks never touch `raw_bcch` at the same time.

### Data quality

Quality is checked twice: at ingestion, before data reaches BigQuery, and
by dbt, on the warehouse.

**At ingestion** (`src/validation/bcch.py`), each series is validated
against its `quality` block in `config/bcch_series.yml`: not empty, valid
and unique dates within the requested window, numeric values, null ratio,
minimum value. Series metadata must be in the BCCh catalog, match the
configured frequency and have curated metadata. A failing series is
rejected whole: its raw files are kept as evidence, nothing is loaded, and
the run fails.

**In dbt**, generic tests in the model YAML cover keys, nulls, accepted
values and relationships. Custom singular tests in
`dbt/chile_financial_analytics/tests/` return every row that breaks a rule
(no rows = pass), grouped in four categories, each a dbt tag:

| Category | Question | Examples |
|---|---|---|
| `structural` | Is the data shaped right? | grain uniqueness, finite values (no NaN/Inf), monthly series dated the 1st |
| `integrity` | Do models agree with each other? | incremental fact equals a full refresh of staging (values and lineage), every series has observations, SCD2 versions never overlap |
| `domain` | Do values make economic sense, per series? | USD/CLP positive and moving < 10 % a day, TPM in 0.25-point steps within [0, 25], IMACEC and IPC within plausible ranges, no weekend values for daily series, no future dates |
| `operational` | Is the pipeline healthy? | no gaps in a series, series not stale for their frequency, no ingestion runs stuck in `running`, every series has a freshness check |

Domain thresholds come from profiling the data since 2020 and are
documented in each test. **Future dates** are judged against extraction
time, not today: BCCh publishes daily values the evening before (Friday's
run already loads Monday's), so a daily observation may be up to 4 days
ahead of its extraction; a monthly one must be for a month already closed.

**Source freshness** is per series, against its update cadence, on
`ingested_at` (when new data last landed in `raw_bcch.observations`). A
single table-level check would let the daily series hide a monthly one
that stopped loading.

| Cadence | Series | warn | error |
|---|---|---|---|
| daily | USD/CLP, TPM | 72 h | 120 h |
| monthly | IMACEC, IPC | 35 days | 45 days |

Thresholds are calendar hours, so Fiestas Patrias (5+ quiet business days)
still trips the daily error once a year.

```bash
cd dbt/chile_financial_analytics
dbt test                                  # everything
dbt test --select tag:domain              # one category
dbt test --select source:monitoring       # the audit table
dbt source freshness                      # per-series freshness
```

In `bcch_financial_pipeline`, `quality` runs `dbt test` after `transform`
(a failing test fails the run), and `freshness` runs `dbt source
freshness` after ingestion, even when dbt is skipped. Stale data is
reported (log, XCom, `airflow_bcch_dbt_nodes_freshness_*` and
`airflow_bcch_hours_since_loaded_*` metrics) without failing the run. Staleness
checks warn rather than fail while the DAG is triggered manually.

**Lineage**: every raw row (`raw_bcch.observations`, `raw_bcch.series`)
and every `fact_economic_observation` row carries `ingestion_run_id`, the
run that last wrote it; its record is in `monitoring.ingestion_runs`.

### Observability

- **Logs**: every line carries `run=<run_id>` (also the run summary's
  file name), `parent=` for pipeline steps, and `airflow=<dag>/<run>/<task>#<try>`
  under Airflow. `LOG_LEVEL` (default `INFO`) and `LOG_FORMAT=json` for
  log aggregators. Airflow task logs are also shipped to
  `gs://$GCS_RAW_BUCKET/airflow-logs/`.
- **Run summaries**: `data/_runs/...`, mirrored to `gs://$GCS_RAW_BUCKET/_runs/...`,
  with status per series, rows changed, step timings and API retries.
- **Audit table**: `monitoring.ingestion_runs` in BigQuery, one row per
  ingestion run, independent of Airflow's metadata database. Written as
  `running` at start and completed at the end, failures included (`failed`,
  or `crashed` if the run raised; a killed run stays `running`). Holds row
  counts per stage (`extracted_rows`, `valid_rows`, `rejected_rows`,
  `loaded_rows`), a short redacted `error_message` (≤ 1,000 characters;
  full detail in the run summary) and the Airflow run. Run ids are unique
  per execution: start timestamp plus a random suffix.

  ```sql
  select
      date(started_at) as run_date,
      status,
      extracted_rows,
      loaded_rows,
      timestamp_diff(completed_at, started_at, second) as duration_seconds
  from `chile-financial-analytics.monitoring.ingestion_runs`
  where source = 'bcch'
  order by started_at desc;
  ```

- **BigQuery jobs** are labelled with the run id and Airflow context
  (ingestion) or dbt invocation id and Airflow context (dbt):

  ```sql
  SELECT job_id, total_bytes_processed
  FROM `region-southamerica-west1`.INFORMATION_SCHEMA.JOBS
  WHERE EXISTS (SELECT 1 FROM UNNEST(labels)
                WHERE key = 'airflow_run_id' AND value LIKE 'manual__2026-10-03%')
  ```

- **Metrics**: Airflow's own plus `bcch.*` (rows changed, durations,
  retries, dbt node statuses) via StatsD, served in Prometheus format at
  http://localhost:9102/metrics.
