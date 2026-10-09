locals {
  name_prefix = "chile-financial-${var.environment}"

  common_labels = {
    project     = "chile-financial-analytics"
    environment = var.environment
    managed_by  = "terraform"
  }
}