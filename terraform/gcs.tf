resource "google_storage_bucket" "raw" {
  name                        = "aframuz-${var.project_id}-raw"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  labels = {
    layer  = "raw"
    source = "bcch"
  }

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  # Raw API responses are kept for audit and replay but rarely read:
  # move them to cheaper storage classes as they age
  lifecycle_rule {
    condition {
      age                   = 90
      matches_prefix        = ["raw/"]
      matches_storage_class = ["STANDARD"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age                   = 365
      matches_prefix        = ["raw/"]
      matches_storage_class = ["NEARLINE"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  # Airflow remote task logs are only useful for recent debugging
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["airflow-logs/"]
    }
    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  to = google_storage_bucket.raw
  id = "chile-financial-analytics/aframuz-chile-financial-analytics-raw"
}
