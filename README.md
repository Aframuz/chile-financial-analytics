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
