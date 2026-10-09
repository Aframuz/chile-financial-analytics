resource "google_project_service" "required" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com", # service account impersonation
    "sts.googleapis.com",            # Workload Identity Federation token exchange
  ])

  service = each.value

  # Other workloads in the project may depend on these APIs
  disable_on_destroy = false
}
