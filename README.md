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

### Observability

- **Logs**: every line carries `run=<run_id>` (also the run summary's
  file name), `parent=` for pipeline steps, and `airflow=<dag>/<run>/<task>#<try>`
  under Airflow. `LOG_LEVEL` (default `INFO`) and `LOG_FORMAT=json` for
  log aggregators. Airflow task logs are also shipped to
  `gs://$GCS_RAW_BUCKET/airflow-logs/`.
- **Run summaries**: `data/_runs/...`, mirrored to `gs://$GCS_RAW_BUCKET/_runs/...`,
  with status per series, rows changed, step timings and API retries.
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
