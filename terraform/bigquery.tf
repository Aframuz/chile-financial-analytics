resource "google_bigquery_dataset" "raw_bcch" {
  dataset_id = "raw_bcch"

  location = var.bigquery_location

  description = "Raw Banco Central de Chile data."

  labels = {
    layer  = "raw"
    source = "bcch"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics_${var.environment}"

  location = var.bigquery_location

  description = "dbt analytical models."

  labels = {
    layer = "analytics"
  }
}

resource "google_bigquery_dataset" "monitoring" {
  dataset_id = "monitoring"

  location = var.bigquery_location

  description = "Operational and ingestion observability data."

  labels = {
    layer = "monitoring"
  }
}

resource "google_bigquery_dataset" "snapshots" {
  dataset_id = "analytics_${var.environment}_snapshots"

  location = var.bigquery_location

  description = "dbt snapshots and historical dimension state."

  labels = {
    layer = "snapshots"
  }
}

import {
  to = google_bigquery_dataset.analytics
  id = "projects/chile-financial-analytics/datasets/analytics_dev"
}
import {
  to = google_bigquery_dataset.monitoring
  id = "projects/chile-financial-analytics/datasets/monitoring"
}

import {
  to = google_bigquery_dataset.snapshots
  id = "projects/chile-financial-analytics/datasets/analytics_dev_snapshots"
}