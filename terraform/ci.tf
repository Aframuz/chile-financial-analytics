# GitHub Actions authenticates with its OIDC token (no key files) through
# Workload Identity Federation, and impersonates one service account per
# trust level (see docs/ci-cd.md):
#
#   gha-ci        unreviewed code: pull requests and pre-apply plans.
#                 Builds dbt into its own analytics_ci_<PR> datasets,
#                 reads raw data, plans Terraform read-only.
#   gha-terraform reviewed code on main: terraform apply.
#   gha-composer  reviewed code on main: syncs DAGs and code to Composer.
#
# Each account trusts exact OIDC subjects, not the whole repository: the
# privileged ones only a job running in a GitHub environment, whose
# protection rules (main only, required reviewers) GitHub enforces before
# the job gets a token.

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
  description               = "OIDC identities from GitHub Actions workflows."

  depends_on = [google_project_service.required]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  display_name                       = "GitHub OIDC"

  # google.subject = "repo:<owner>/<repo>:<context>", where context is
  # pull_request, ref:refs/heads/<branch> or environment:<name>. Only
  # claims present in every token can be mapped: no `environment` here,
  # the subject already carries it.
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # Without a condition, any GitHub repository could use this provider
  attribute_condition = "assertion.repository == \"${var.github_repository}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

locals {
  github_subject_prefix = "principal://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/subject/repo:${var.github_repository}"

  # Service account => OIDC subjects allowed to impersonate it
  github_impersonation = {
    ci = [
      "environment:ci",      # dbt builds and dataset cleanup
      "pull_request",        # terraform plan on pull requests
      "ref:refs/heads/main", # terraform plan before apply
    ]
    terraform = [
      "environment:infra-auto",     # low-risk plans, applied on merge
      "environment:infra-approval", # everything else, after review
    ]
    composer = [
      "environment:composer",
    ]
  }

  # Must match the backend bucket in versions.tf
  tfstate_bucket = "aframuz-chile-financial-analytics-tfstate"
}

resource "google_service_account" "github" {
  for_each = local.github_impersonation

  account_id   = "gha-${each.key}"
  display_name = "GitHub Actions ${each.key}"
}

resource "google_service_account_iam_member" "github_workload_identity" {
  for_each = {
    for pair in flatten([
      for account, subjects in local.github_impersonation : [
        for subject in subjects : { account = account, subject = subject }
      ]
    ]) : "${pair.account}/${pair.subject}" => pair
  }

  service_account_id = google_service_account.github[each.value.account].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.github_subject_prefix}:${each.value.subject}"
}

# ==================================
# gha-ci: per-PR dbt datasets, read-only everything else
# ==================================

# bigquery.user: run jobs and create datasets. BigQuery makes the creator
# of a dataset its owner, so CI owns (and can delete) the analytics_ci_*
# datasets it creates, and nothing else: no grant on analytics_dev.
resource "google_project_iam_member" "ci" {
  for_each = toset([
    "roles/bigquery.user",
    # terraform plan reads every managed resource and its IAM policy
    "roles/viewer",
    "roles/iam.securityReviewer",
  ])

  project = var.project_id
  role    = each.value
  member  = google_service_account.github["ci"].member
}

# dbt sources
resource "google_bigquery_dataset_iam_member" "ci_viewer" {
  for_each = {
    raw_bcch   = google_bigquery_dataset.raw_bcch.dataset_id
    monitoring = google_bigquery_dataset.monitoring.dataset_id
  }

  dataset_id = each.value
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.github["ci"].member
}

# Plans read state but never lock it (terraform plan -lock=false)
resource "google_storage_bucket_iam_member" "ci_tfstate_reader" {
  bucket = local.tfstate_bucket
  role   = "roles/storage.objectViewer"
  member = google_service_account.github["ci"].member
}

# ==================================
# gha-terraform: applies this configuration
# ==================================

# Whatever this configuration manages. Project IAM admin makes this account
# as powerful as an owner: it is only reachable from the infra-*
# environments, i.e. from reviewed code on main.
resource "google_project_iam_member" "terraform" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.workloadIdentityPoolAdmin",
    "roles/resourcemanager.projectIamAdmin",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/composer.admin",
    "roles/secretmanager.admin",
  ])

  project = var.project_id
  role    = each.value
  member  = google_service_account.github["terraform"].member
}

# Creating the Composer environment attaches financial-composer to it
resource "google_service_account_iam_member" "terraform_acts_as_composer" {
  service_account_id = google_service_account.composer.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.github["terraform"].member
}

# ==================================
# gha-composer: continuous delivery to Cloud Composer
# ==================================

# Describe the environment (Airflow version, bucket) and run Airflow CLI
# commands (post-deploy import check)
resource "google_project_iam_member" "composer_user" {
  project = var.project_id
  role    = "roles/composer.user"
  member  = google_service_account.github["composer"].member
}

# Sync dags/ and data/ in the environment's bucket, nothing else. The
# bucket outlives the environment (composer.tf), so this grant does too.
resource "google_storage_bucket_iam_member" "composer_bucket" {
  bucket = google_storage_bucket.composer.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.github["composer"].member
}
