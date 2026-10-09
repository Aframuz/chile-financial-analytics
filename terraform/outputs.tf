output "raw_bucket_name" {
  description = "GCS bucket used for raw source data."
  value       = google_storage_bucket.raw.name
}

output "raw_bcch_dataset" {
  description = "BigQuery dataset containing raw BCCh data."
  value       = google_bigquery_dataset.raw_bcch.dataset_id
}

output "analytics_dataset" {
  description = "BigQuery dataset containing analytical models."
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "ingestion_service_account" {
  description = "Service account used by ingestion workloads."
  value       = google_service_account.ingestion.email
}

output "dbt_service_account" {
  description = "Service account used by dbt locally and in CI."
  value       = google_service_account.dbt.email
}

output "workload_identity_provider" {
  description = "Full provider name for google-github-actions/auth."
  value       = google_iam_workload_identity_pool_provider.github.name
}