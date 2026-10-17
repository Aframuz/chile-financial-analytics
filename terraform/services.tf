resource "google_project_service" "required" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com", # service account impersonation
    "sts.googleapis.com",            # Workload Identity Federation token exchange
    "composer.googleapis.com",       # on even without an environment: the deploy checks for one
    "secretmanager.googleapis.com",  # BCCh token for Composer
  ])

  service = each.value

  # Other workloads in the project may depend on these APIs
  disable_on_destroy = false
}
