resource "google_service_account" "ingestion" {
  account_id = "financial-ingestion"

  display_name = "Chile Financial Analytics Ingestion"
}

resource "google_service_account" "dbt" {
  account_id = "financial-dbt"

  display_name = "Chile Financial Analytics dbt"
}

# Loads, queries and MERGEs run as jobs billed to the project, so jobUser
# only exists at project level. Data access is granted per dataset below.
resource "google_project_iam_member" "bigquery_job_user" {
  for_each = {
    ingestion = google_service_account.ingestion.member
    dbt       = google_service_account.dbt.member
  }

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = each.value
}

# ==================================
# Ingestion: raw files + raw/monitoring tables
# ==================================

# Raw files, run summaries and Airflow remote logs. Reruns overwrite
# objects, which needs delete: objectUser rather than objectCreator.
resource "google_storage_bucket_iam_member" "ingestion_raw_bucket" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectUser"
  member = google_service_account.ingestion.member
}

# Creates tables and _staging_* tables, loads, MERGEs and deletes
resource "google_bigquery_dataset_iam_member" "ingestion_editor" {
  for_each = {
    raw_bcch   = google_bigquery_dataset.raw_bcch.dataset_id
    monitoring = google_bigquery_dataset.monitoring.dataset_id
  }

  dataset_id = each.value
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.ingestion.member
}

# ==================================
# dbt: read sources, build analytics + snapshots
# ==================================

resource "google_bigquery_dataset_iam_member" "dbt_viewer" {
  for_each = {
    raw_bcch   = google_bigquery_dataset.raw_bcch.dataset_id
    monitoring = google_bigquery_dataset.monitoring.dataset_id
  }

  dataset_id = each.value
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.dbt.member
}

resource "google_bigquery_dataset_iam_member" "dbt_editor" {
  for_each = {
    analytics = google_bigquery_dataset.analytics.dataset_id
    snapshots = google_bigquery_dataset.snapshots.dataset_id
  }

  dataset_id = each.value
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.dbt.member
}

# ==================================
# Local development: impersonation instead of key files
# ==================================

resource "google_service_account_iam_member" "operator_token_creator" {
  for_each = {
    ingestion = google_service_account.ingestion.name
    dbt       = google_service_account.dbt.name
  }

  service_account_id = each.value
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.operator_email}"
}