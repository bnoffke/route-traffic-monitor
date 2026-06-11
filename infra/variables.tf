variable "project" {
  type        = string
  default     = "madison-municipal-data"
  description = "GCP project ID"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region for Cloud Run and Scheduler"
}

variable "bucket" {
  type        = string
  default     = "stmsn-bronze"
  description = "Pre-existing GCS bucket for Parquet output"
}
