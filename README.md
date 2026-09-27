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
