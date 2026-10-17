# Cloud Composer, run on demand for demos (docs/ci-cd.md, "Setting up
# Cloud Composer").
#
# Only the environment comes and goes, with composer_enabled in
# composer.auto.tfvars: a reviewed PR, a high-risk plan approved in
# infra-approval. Everything it relies on stays when it's deleted and costs
# next to nothing: the bucket, the identity and its grants, the BCCh token
# secret. Recreating it is one flag, and CI keeps working in between (the
# deploy skips a missing environment).

locals {
  composer_environment_name = "chile-financial-airflow"

  # Workers see the bucket's data/ folder here
  composer_project_dir = "/home/airflow/gcs/data/project"

  # The Airflow version CI tests the DAGs against
  pinned_airflow_version = regex(
    "ARG AIRFLOW_VERSION=([0-9.]+)",
    file("${path.module}/../airflow/Dockerfile"),
  )[0]

  # The runtime deps of src/, from the same file the local image installs
  composer_pypi_packages = {
    for match in regexall(
      "(?m)^([A-Za-z0-9_.-]+)==([^\\s#]+)",
      file("${path.module}/../airflow/requirements.txt"),
    ) : lower(match[0]) => "==${match[1]}"
  }
}

data "google_project" "current" {}

# ==================================
# Persistent: survive the environment's deletion
# ==================================

# The environment's bucket (dags/, data/, logs/), set explicitly rather
# than generated, so its name and IAM grants don't change on every
# recreation. Snapshots go here too.
resource "google_storage_bucket" "composer" {
  name                        = "aframuz-${var.project_id}-composer"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  labels = {
    layer = "orchestration"
  }

  # Per-task dbt artefacts (bcch_common.dbt_run_paths) accumulate
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["data/project/dbt/chile_financial_analytics/target/"]
    }
    action {
      type = "Delete"
    }
  }
}

# Airflow tasks run as this account: what ingestion and dbt need
resource "google_service_account" "composer" {
  account_id   = "financial-composer"
  display_name = "Chile Financial Analytics Composer workers"
}

resource "google_project_iam_member" "composer_runtime" {
  for_each = toset([
    "roles/composer.worker",
    "roles/bigquery.jobUser",
  ])

  project = var.project_id
  role    = each.value
  member  = google_service_account.composer.member
}

resource "google_bigquery_dataset_iam_member" "composer_editor" {
  for_each = {
    raw_bcch   = google_bigquery_dataset.raw_bcch.dataset_id
    monitoring = google_bigquery_dataset.monitoring.dataset_id
    analytics  = google_bigquery_dataset.analytics.dataset_id
    snapshots  = google_bigquery_dataset.snapshots.dataset_id
  }

  dataset_id = each.value
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.composer.member
}

resource "google_storage_bucket_iam_member" "composer_raw_bucket" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectUser"
  member = google_service_account.composer.member
}

resource "google_storage_bucket_iam_member" "composer_environment_bucket" {
  bucket = google_storage_bucket.composer.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.composer.member
}

# Composer's Google-managed agent creates and operates environments that
# run as a user-managed service account
resource "google_project_iam_member" "composer_service_agent" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.current.number}@cloudcomposer-accounts.iam.gserviceaccount.com"

  depends_on = [google_project_service.required]
}

# The BCCh token, as the Airflow Variable `bcch_api_token` through
# Composer's Secret Manager backend (bcch_common.runtime_env). Terraform
# owns the secret, never its value:
#   printf '%s' "$BCCH_API_TOKEN" | gcloud secrets versions add \
#     airflow-variables-bcch_api_token --data-file=-
resource "google_secret_manager_secret" "bcch_api_token" {
  secret_id = "airflow-variables-bcch_api_token"

  replication {
    auto {}
  }

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_iam_member" "composer_bcch_api_token" {
  secret_id = google_secret_manager_secret.bcch_api_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.composer.member
}

# ==================================
# The environment: only while composer_enabled
# ==================================

resource "google_composer_environment" "airflow" {
  count = var.composer_enabled ? 1 : 0

  name   = local.composer_environment_name
  region = var.region

  storage_config {
    bucket = google_storage_bucket.composer.name
  }

  config {
    software_config {
      image_version = var.composer_image_version
      pypi_packages = local.composer_pypi_packages

      # Mirrors docker-compose.airflow.yml. No secrets here: anyone who can
      # view the environment can read these.
      env_variables = {
        PROJECT_DIR                 = local.composer_project_dir
        GCP_PROJECT_ID              = var.project_id
        GCS_RAW_BUCKET              = google_storage_bucket.raw.name
        STORAGE_BACKEND             = "gcs"
        BIGQUERY_ENABLED            = "true"
        BIGQUERY_RAW_DATASET        = google_bigquery_dataset.raw_bcch.dataset_id
        BIGQUERY_MONITORING_DATASET = google_bigquery_dataset.monitoring.dataset_id
        DBT_PROJECT_DIR             = "${local.composer_project_dir}/dbt/chile_financial_analytics"
        DBT_PROFILES_DIR            = "${local.composer_project_dir}/dbt/chile_financial_analytics/profiles"
        DBT_TARGET                  = "airflow"
        DBT_DATASET                 = google_bigquery_dataset.analytics.dataset_id
        DBT_BQ_LOCATION             = var.bigquery_location
        # No DBT_BIN: tasks build dbt's venv from this lock (bcch_common.dbt_bin)
        DBT_REQUIREMENTS = "${local.composer_project_dir}/airflow/requirements-dbt.txt"
      }

      airflow_config_overrides = {
        "core-dags_are_paused_at_creation" = "True"
        "secrets-backend"                  = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
      }
    }

    # The cost lever: every value below is billed as DCUs. Sized for one
    # light pipeline with a 1-slot pool; check Composer 3's minimums.
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      dag_processor {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      triggerer {
        cpu       = 0.5
        memory_gb = 1
        count     = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu       = 1
        memory_gb = 4
        # Room for the dbt venv built on first use
        storage_gb = 5
        min_count  = 1
        max_count  = 1 # one BigQuery task at a time anyway (pool of 1)
      }
    }

    node_config {
      service_account = google_service_account.composer.email
    }
  }

  lifecycle {
    precondition {
      condition     = strcontains(var.composer_image_version, "-airflow-${local.pinned_airflow_version}-")
      error_message = "composer_image_version must run Airflow ${local.pinned_airflow_version}, the version pinned in airflow/Dockerfile (e.g. composer-3-airflow-${local.pinned_airflow_version}-build.N)."
    }
  }

  depends_on = [
    google_project_iam_member.composer_service_agent,
    google_project_iam_member.composer_runtime,
    google_storage_bucket_iam_member.composer_environment_bucket,
  ]
}
