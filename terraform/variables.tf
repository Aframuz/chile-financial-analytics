variable "project_id" {
  description = "GCP project ID for the Chile Financial Analytics Platform."
  type        = string
}

variable "region" {
  description = "Default GCP region."
  type        = string
  default     = "southamerica-west1"
}

variable "bigquery_location" {
  description = "Location for BigQuery datasets."
  type        = string
  default     = "southamerica-west1"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}

variable "operator_email" {
  description = "Google account allowed to impersonate the workload service accounts."
  type        = string
}

variable "github_repository" {
  description = "GitHub repository (owner/name) allowed to authenticate through Workload Identity Federation."
  type        = string
}