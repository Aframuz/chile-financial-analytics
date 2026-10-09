provider "google" {
  project = var.project_id
  region  = var.region

  # Applied to every resource that supports labels; resources add their
  # own (layer, source) on top
  default_labels = local.common_labels
}