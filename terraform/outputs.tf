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
  description = "Service account used by dbt locally and in Airflow."
  value       = google_service_account.dbt.email
}

output "workload_identity_provider" {
  description = "Full provider name for google-github-actions/auth."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "github_service_accounts" {
  description = "Service accounts GitHub Actions impersonates, by trust level."
  value       = { for name, account in google_service_account.github : name => account.email }
}

# Paste into GitHub (see docs/ci-cd.md): gh variable set NAME --body VALUE
output "github_repository_variables" {
  description = "Repository variables the workflows read."
  value = {
    GCP_PROJECT_ID                 = var.project_id
    GCP_WORKLOAD_IDENTITY_PROVIDER = google_iam_workload_identity_pool_provider.github.name
    GCP_CI_SERVICE_ACCOUNT         = google_service_account.github["ci"].email
    GCP_TERRAFORM_SERVICE_ACCOUNT  = google_service_account.github["terraform"].email
    GCP_COMPOSER_SERVICE_ACCOUNT   = google_service_account.github["composer"].email
    BQ_LOCATION                    = var.bigquery_location
    # Constant whether or not the environment exists: the deploy skips
    # when it doesn't
    COMPOSER_ENVIRONMENT = local.composer_environment_name
    COMPOSER_LOCATION    = var.region
  }
}

output "composer_bucket" {
  description = "Cloud Composer environment bucket; persists when the environment is deleted."
  value       = google_storage_bucket.composer.name
}

output "composer_airflow_uri" {
  description = "Airflow UI, while the environment exists."
  value       = one(google_composer_environment.airflow[*].config[0].airflow_uri)
}
