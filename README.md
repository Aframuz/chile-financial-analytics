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
