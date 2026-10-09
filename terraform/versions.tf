terraform {
  required_version = ">= 1.16"

  # Bucket created once with gcloud (see README): the backend must exist
  # before init, so this configuration can't manage it
  backend "gcs" {
    bucket = "aframuz-chile-financial-analytics-tfstate"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.0"
    }
  }
}