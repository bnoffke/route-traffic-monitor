provider "google" {
  project = var.project
  region  = var.region
}

locals {
  cfg = yamldecode(file("${path.module}/../config/routes.yaml"))
}

# ---------------------------------------------------------------------------
# Reference the pre-existing bronze bucket — never manage its lifecycle here
# ---------------------------------------------------------------------------
data "google_storage_bucket" "bronze" {
  name = var.bucket
}

# ---------------------------------------------------------------------------
# API enablement
# ---------------------------------------------------------------------------
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
  ])

  service            = each.key
  disable_on_destroy = false
}

# ---------------------------------------------------------------------------
# Service accounts
# ---------------------------------------------------------------------------
resource "google_service_account" "runner" {
  account_id   = "route-traffic-runner"
  display_name = "Route Traffic — Cloud Run Job runner"
}

resource "google_service_account" "scheduler" {
  account_id   = "route-traffic-scheduler"
  display_name = "Route Traffic — Cloud Scheduler invoker"
}

# ---------------------------------------------------------------------------
# IAM: runner → GCS bucket (objectAdmin for idempotent overwrites on retry)
# ---------------------------------------------------------------------------
resource "google_storage_bucket_iam_member" "runner_gcs" {
  bucket = data.google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.runner.email}"
}

# ---------------------------------------------------------------------------
# IAM: runner → Secret Manager (read the API key)
# ---------------------------------------------------------------------------
resource "google_secret_manager_secret_iam_member" "runner_secret" {
  secret_id = google_secret_manager_secret.routes_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.runner.email}"
}

# NOTE: scheduler → Cloud Run Job `run.invoker` binding is granted by scripts/deploy.sh,
# not Terraform. The job is created by `gcloud run jobs deploy`, so a TF binding here would
# 404 on first apply (the job doesn't exist yet). Keeping it with the gcloud-owned job
# (deploy.sh) lets `terraform apply` run clean in a single pass.

# ---------------------------------------------------------------------------
# Secret Manager: container only (value added manually via gcloud)
# ---------------------------------------------------------------------------
resource "google_secret_manager_secret" "routes_api_key" {
  secret_id = "routes-api-key"

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

# ---------------------------------------------------------------------------
# Cloud Scheduler jobs — one per schedule block in routes.yaml
# ---------------------------------------------------------------------------
resource "google_cloud_scheduler_job" "poll" {
  for_each = { for s in local.cfg.schedules : s.name => s }

  name      = "route-traffic--${each.key}"
  schedule  = each.value.cron
  time_zone = local.cfg.timezone
  region    = var.region

  # Plain :run (no overrides body). An overrides body would require the extra
  # run.jobs.runWithOverrides permission; we keep the SA on plain roles/run.invoker
  # and derive the schedule label downstream from the run's local time-of-day.
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project}/locations/${var.region}/jobs/route-traffic:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }

  depends_on = [google_project_service.apis]
}
