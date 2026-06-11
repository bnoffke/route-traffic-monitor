#!/usr/bin/env bash
set -euo pipefail

PROJECT=${1:-$(gcloud config get-value project)}
REGION=us-central1

gcloud run jobs deploy route-traffic \
  --source . \
  --region "$REGION" \
  --service-account "route-traffic-runner@${PROJECT}.iam.gserviceaccount.com" \
  --set-secrets ROUTES_API_KEY=routes-api-key:latest \
  --set-env-vars "BUCKET=stmsn-bronze,PREFIX=route-traffic/madison" \
  --max-retries 1 \
  --task-timeout 5m

# Grant the scheduler SA permission to invoke this job (idempotent).
# Lives here rather than Terraform because the job is created by gcloud above.
gcloud run jobs add-iam-policy-binding route-traffic \
  --region "$REGION" \
  --member="serviceAccount:route-traffic-scheduler@${PROJECT}.iam.gserviceaccount.com" \
  --role=roles/run.invoker
