# Architecture: Madison Route Traffic Poller

**Status:** Design approved for implementation
**Owner:** Ben
**Purpose of this document:** Hand-off spec for developer agents to produce a working solution. It specifies service choices, configuration schema, output contract, IAM, and a deployment runbook. Where a decision was a judgment call, the tradeoff is recorded so implementers don't relitigate it.

---

## 1. Goal

Poll the Google Routes API (`computeRoutes`) for a fixed set of Madison street segments on a schedule concentrated around peak commute hours, and land the raw responses as Parquet in the bronze GCS bucket for downstream dbt/DuckDB consumption.

This replaces a prior R + GitHub Actions implementation (attached reference: `get_route()` script). The reference script defines the request shape, field mask, and the route inventory; this design ports it to Python on GCP-native scheduling and compute. One deliberate departure: waypoints are specified as lat/lng coordinates (`location.latLng`, optional `heading`) rather than the reference script’s place IDs — coordinates are stable over time, easier to author, and support directional headings. The bronze layer should capture what the API returned plus run metadata — *no* derived columns (delay, day-of-week, etc.). Those belong in dbt/silver.

## 2. High-level architecture

```
Cloud Scheduler (1 cron job on the window cron, America/Chicago)
        │  authenticated POST to Cloud Run Admin API ("run job now")
        ▼
Cloud Run Job  (Python 3.12, deployed from source via Buildpacks)
        │  reads routes.yaml (baked into image)
        │  reads ROUTES_API_KEY from Secret Manager (env var injection)
        │  one POST per route to routes.googleapis.com computeRoutes
        ▼
GCS: gs://stmsn-bronze/route-traffic/madison/dt=YYYY-MM-DD/<run_ts>.parquet
```

One scheduler tick = one job execution = one parquet file containing one row per (route × returned alternative).

## 3. Service selection and rationale

**Compute: Cloud Run Jobs.** This is a run-to-completion batch task. Cloud Run Jobs is GCP's primitive for exactly that: no HTTP server shim, configurable task timeout and `--max-retries`, and crucially it supports **source-based deployment** (`gcloud run jobs deploy --source .`), which uses Google Cloud Buildpacks to containerize a plain Python project (`main.py` + `requirements.txt` + a `Procfile` is optional). No Dockerfile is required. This satisfies the "ease of deployment is priority one" constraint.

Alternatives considered:

*Cloud Run functions (2nd gen).* Equally easy to deploy from source and a perfectly valid choice. Rejected only because it forces an HTTP handler signature and an OIDC-authenticated Scheduler→URL hop for what is conceptually a batch job. If implementers hit any friction with Jobs, Functions is the sanctioned fallback — the application code should be structured so the entrypoint wrapper is the only thing that changes (see §10 repo layout).

*Cloud Run service (always-on).* Wrong shape; pays for idle, needs an HTTP server.

*GitHub Actions cron (status quo).* Rejected: cron schedules are UTC-only (DST drift on "peak hours"), start times are best-effort and routinely 5–30 min late, secrets live outside GCP IAM, and writing to GCS requires bootstrapping workload identity federation anyway. Once you need GCP credentials, you may as well run on GCP.

**Scheduler: Cloud Scheduler.** Timezone-aware cron (`America/Chicago`), so peak windows survive DST transitions. A **single** Scheduler job fires on `poll_window_cron`, a superset of the schedule blocks; the job itself matches the local time-of-day against those blocks and exits early when none is active (§6, `src/schedule.py`). One cron cannot express the exact union of the blocks — cron fields are a cross-product, and the weekday hours differ from the weekend hours — and Scheduler bills per job, so one job plus an in-process gate is cheaper than five jobs. It triggers the Cloud Run Job via an authenticated POST to
`https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/jobs/{JOB}:run`
using OAuth (service account with `roles/run.invoker` on the job). This is the documented Scheduler→Jobs pattern.

**Storage: GCS, Parquet.** See §6.

**Orchestrator: none, deliberately.** This does not run under Dagster. It is a self-contained leaf producer writing to bronze; the existing Dagster/dbt stack consumes the bucket as an external/source asset. Keeping the poller orchestrator-free means it can't be broken by Dagster deploys and vice versa. (Future option in §11.)

## 4. Configuration

A single `config/routes.yaml` **in the repo, baked into the deploy**. Editing config = edit file, run the deploy command (≈2 min). 

*Tradeoff acknowledged:* a bucket-hosted config (`gs://.../config.yaml`) would allow route changes without redeploy, but introduces a runtime fetch, a second versioning surface, and "which config is actually live?" ambiguity. With a single-maintainer project and a one-command deploy, repo config is simpler and self-documenting via git history. Revisit only if config churn becomes frequent or a non-deploying collaborator needs to edit routes.

```yaml
# config/routes.yaml
defaults:
  travel_mode: DRIVE
  routing_preference: TRAFFIC_AWARE
  compute_alternative_routes: true
  language_code: en-US
  units: METRIC

# Named places as coordinates (right-click -> copy coordinates in Google Maps).
# `heading` (0-360, degrees clockwise from north = intended direction of travel)
# is OPTIONAL but REQUIRED on divided roads (e.g., E Wash) to prevent the router
# snapping the point to the opposite carriageway.
places:
  JND_Rimrock_inbound: { lat: 43.0411, lng: -89.3786 }              # illustrative coords
  Hairball_inbound:    { lat: 43.0660, lng: -89.3812, heading: 350 }
  # ... maintainer supplies coordinates for the v1 routes

# Route segments to poll. `intermediate` is optional.
routes:
  - name: JND_rimrock_to_hairball
    corridor: "John Nolen Dr (Rimrock <-> Hairball)"
    direction: NB
    origin: JND_Rimrock_inbound
    destination: Hairball_inbound
  - name: park_inbound
    corridor: "Park (Badger <-> University)"
    direction: NB
    origin: Park_Badger_inbound
    destination: Park_University_inbound
    # intermediate: Some_Place      # supported, optional
  # ... one entry per directional segment (~16 total)

timezone: America/Chicago

# The one Cloud Scheduler job Terraform materializes fires on this cron (infra/ reads this
# file via yamldecode; see §11). Must be a superset of every block below; load_config()
# refuses to start if it stops covering one.
poll_window_cron: "*/10 6-9,12,15-19 * * *"

# Real polling slots. Declared here, enforced in-process by src/schedule.py rather than by
# Terraform, and used as the `schedule_name` label. Adding a check = add a block here, widen
# poll_window_cron if needed, terraform apply.
schedules:
  - name: am-peak
    cron: "*/10 6-9 * * 1-5"      # every 10 min, 6:00–9:50, weekdays (cheap at 2 routes; see §9)
  - name: pm-peak
    cron: "*/10 15-18 * * 1-5"    # every 10 min, 15:00–18:50, weekdays
  - name: pm-shoulder
    cron: "0 19 * * 1-5"          # single 7:00 PM sample; continuity with predecessor dataset's shoulder observation
  - name: midday-baseline
    cron: "0 12 * * 1-5"          # free-flow-ish comparison point
  - name: weekend-baseline
    cron: "0 12,17 * * 6,0"
```

Notes for implementers:

- `corridor` and `direction` ride along as columns in the output so downstream models don't need the brittle `case_when(origin == ...)` mapping from the R script. The mapping is maintained in one place: the config.
- Validate config at startup (e.g., pydantic): every `origin`/`destination`/`intermediate` must resolve to a key in `places`; route `name` values must be unique and `[a-z0-9_]+`.
- Terraform is the only mechanism that creates/updates Scheduler jobs (it `yamldecode`s this file), so the YAML remains the single source of truth and removed schedule blocks are pruned automatically by `terraform apply`.

## 5. The API request (contract with Routes API)

Port the R script's request verbatim. Endpoint: `POST https://routes.googleapis.com/directions/v2:computeRoutes`. Headers:

```
Content-Type: application/json
X-Goog-Api-Key: <from Secret Manager>
X-Goog-FieldMask: routes.duration,routes.description,routes.staticDuration,routes.distanceMeters,routes.polyline.encodedPolyline
```

Body (per route):

```json
{
  "origin": {
    "location": {
      "latLng": {"latitude": <places[route.origin].lat>, "longitude": <places[route.origin].lng>},
      "heading": <places[route.origin].heading>            // include only if set in config
    }
  },
  "destination":   { "location": { "latLng": {...} } },     // same shape
  "intermediates": [{ "location": { "latLng": {...} } }],   // omit key entirely if route has no intermediate
  "travelMode": "DRIVE",
  "routingPreference": "TRAFFIC_AWARE",
  "computeAlternativeRoutes": true,
  "languageCode": "en-US",
  "units": "METRIC"
}
```

Implementation notes:

- Keep the field mask **exactly** this lean. The field mask affects which SKU bills (e.g., requesting `routes.travelAdvisory.tollInfo` escalates the SKU). Do not add fields casually.
- One response may contain multiple routes (`computeAlternativeRoutes: true`); emit one output row per returned route, same as the R script.
- **Coordinate snapping caveat:** bare lat/lng points snap to the nearest road edge; on divided roads this can silently select the wrong carriageway (measuring the opposite direction, often with a U-turn added). Defenses: coordinates placed on the correct carriageway, `heading` set on divided-road waypoints, and `sideOfRoad: true` on the Waypoint if needed. A wrong-carriageway snap typically manifests as anomalous `distanceMeters`; log a WARN if a route's primary-alternative distance deviates >25% from a per-route `expected_distance_m` value in config (optional field, recommended).
- Use `httpx` (or `requests`) with a 30s timeout and 2–3 retries with backoff on 5xx/429 per route. A route that still fails is logged at ERROR with the route name and **skipped** — one bad place ID must not sink the whole run. If *all* routes fail, exit nonzero so the Job execution is marked failed.
- The ~16 requests can be made sequentially; total runtime is a few seconds. Concurrency is unnecessary complexity here.

## 6. Output contract

**Format: Parquet.** Downstream is DuckDB/dbt, which reads GCS parquet natively; columnar, typed, compact, and avoids the CSV type-inference issues the R script works around (`"698s"` duration strings, etc.).

**Layout: one file per run, hive-partitioned by date:**

```
gs://stmsn-bronze/route-traffic/madison/dt=2026-06-11/run_ts=2026-06-11T1230Z.parquet
```

*Tradeoff vs. the proposed `.../madison/{route-name}/` layout:* per-route paths would create ~16 tiny files per run (small-file proliferation: slower listings, more GETs, worse scan efficiency), and the dominant downstream access pattern is "all routes over a time range," not "one route's full history." With route name as a column, DuckDB does `read_parquet('gs://stmsn-bronze/route-traffic/madison/**/*.parquet', hive_partitioning=true)` and filters with a plain WHERE; date partitioning gives partition pruning on the axis that actually grows. Single-route filtering on these data volumes is trivial either way.

**Run timestamp = execution start time truncated to the minute, UTC.** The container cannot see Cloud Scheduler's intended schedule time (Scheduler's `X-CloudScheduler-ScheduleTime` header is attached to its call to the Cloud Run Admin API, not propagated into the container), so the execution's own start time is the best available logical-run identifier. Truncating to the minute makes Cloud Run task retries idempotent: retries fire within seconds, compute the same object name, and overwrite rather than duplicate. Residual edge cases (manual re-execution, a retry crossing a minute boundary) can produce a second file; handle with a dedupe in the dbt staging layer (latest `request_time_utc` per `route_name` × truncated run timestamp). The `dt=` partition is the local calendar date (config `timezone`, America/Chicago) of the same timestamp, so the 7PM run lands in its own day rather than rolling into the next UTC day.

**Schema** (one row per route × alternative; raw API values, no derived fields):

| column | type | source |
|---|---|---|
| route_name | string | config |
| corridor | string | config |
| direction | string | config |
| origin | string | config (place key) |
| destination | string | config (place key) |
| intermediate | string (nullable) | config |
| alternative_index | int32 | position in `routes[]` response array |
| description | string | API |
| distance_meters | int32 | API |
| duration_seconds | int32 | API (`"698s"` → strip `s`, cast) |
| static_duration_seconds | int32 | API |
| encoded_polyline | string | API |
| request_time_utc | timestamp (UTC) | runtime |
| schedule_name | string (nullable) | passed by scheduler payload, see below |
| run_id | string | Cloud Run execution ID (`CLOUD_RUN_EXECUTION` env var) |

Parsing `duration`/`staticDuration` to integer seconds at ingest is the one transformation permitted in bronze — it is lossless type coercion, not derivation. Delay = `duration - static_duration`, minutes, miles, day-of-week, weekend flags, and local-time columns are all dbt's job.

`schedule_name`: derived in-process. `src/schedule.py:active_schedule` matches the run's local time-of-day against the `schedules` blocks and returns the one that fired; `main()` writes its name onto every row. This avoids the Jobs run API `overrides.containerOverrides[].env` route, which needs the extra `run.jobs.runWithOverrides` permission (tried and reverted in 2dc3460). Setting `SCHEDULE_NAME` in the environment overrides the match and bypasses the gate, which is how a manual off-slot `gcloud run jobs execute` still polls.

## 7. Secrets and IAM

- **API key** lives in Secret Manager (`routes-api-key`), injected into the Job as env var `ROUTES_API_KEY` via `--set-secrets`. Never in the repo, never in the image. Restrict the key in the Maps console to the Routes API only.
- **Job runtime service account** (`route-traffic-runner@...`): `roles/storage.objectCreator` (or objectAdmin if overwrites on retry are desired — they are, so objectAdmin scoped to the bucket, or objectUser) on `stmsn-bronze`, plus `roles/secretmanager.secretAccessor` on the one secret.
- **Scheduler service account** (`route-traffic-scheduler@...`): `roles/run.invoker` on the Cloud Run Job. Scheduler authenticates the `:run` POST with an OAuth token for this SA.
- Enable APIs: `run.googleapis.com`, `cloudscheduler.googleapis.com`, `secretmanager.googleapis.com`, `cloudbuild.googleapis.com` (source deploys), `artifactregistry.googleapis.com`, plus `routes.googleapis.com` on whichever project holds the Maps key.

## 8. dlt: evaluated and deferred

dlt was considered (maintainer wants reps with it). Verdict: **not for v1.** dlt's value is schema inference/evolution, incremental & merge loading, pagination, and state management. This pipeline has a fixed 14-column schema, append-only writes, no pagination, and no incremental state — dlt's filesystem destination would add `.dlt/` config conventions, credential-resolution indirection, and pipeline state files to a job that is otherwise ~150 lines of obvious Python. It would not *break* the deployment story, but it adds concepts without exercising the features that make dlt worth learning. Better dlt learning vehicle: the Census ACS ingestion (real pagination + incremental cursors). If dlt is adopted later, the seam is clean: replace the "write parquet to GCS" function with a dlt pipeline targeting the filesystem destination; nothing upstream changes.

## 9. Cost

GCP side is effectively free: ~30–60 Job executions/day at a few seconds each (Cloud Run free tier covers it many times over), Scheduler jobs are $0.10/job/month beyond 3 free, GCS storage is pennies.

**The real budget line is the Routes API.** `TRAFFIC_AWARE` requests bill under the Pro SKU (not Essentials), and Pro SKUs carry a 5,000-events/month free threshold under the March-2025+ pricing model. Billing is per request — `computeAlternativeRoutes: true` returning multiple routes costs nothing extra.

**v1 launches with 2 routes**, which sits comfortably inside the free threshold even at high resolution: 2 routes × every 5 min × two 4-hour peak windows ≈ 4,100 requests/month; every 10 min ≈ 2,050. Default the v1 schedules to **every 5–10 minutes** — finer delay curves are more useful analytically and cost nothing at this scale.

The cost cliff appears when scaling back toward the full ~16-route inventory; cadence math at 16 routes:

| cadence (peak windows only, 2×4h weekdays) | req/weekday | req/month (~21 wd) |
|---|---|---|
| every 10 min | 1,024 | ~21,500 |
| every 15 min | 512 + baselines | ~11,500 |
| every 20 min | 384 + baselines | ~8,700 |
| every 30 min | 256 + baselines | ~5,900 |

At ~$10/1,000 (Pro, lowest volume tier), 16 routes at 15-min cadence runs roughly $60–70/month. **Implementers: cadence must be purely a config/schedule decision so it can be retuned as routes are added. Maintainer: verify current Compute Routes Pro pricing in the Maps console and set a billing budget alert on the Maps project before enabling schedules.**

## 10. Repo layout & code structure

```
route-traffic/
├── config/
│   └── routes.yaml          # single source of truth: places, routes, schedules
├── src/
│   ├── main.py              # entrypoint: load config → poll → write parquet → exit code
│   ├── config.py            # pydantic models + validation
│   ├── routes_client.py     # computeRoutes call, retries, response → records
│   └── sink.py              # records → parquet (pyarrow) → GCS
├── infra/
│   ├── main.tf              # services, SAs, IAM, secret container, scheduler job
│   ├── variables.tf
│   └── versions.tf
├── scripts/
│   └── deploy.sh            # wraps gcloud run jobs deploy --source (the only non-TF step)
├── tests/
│   └── test_parsing.py      # response-fixture → records; config validation
├── pyproject.toml           # deps: httpx, pyyaml, pydantic, pyarrow, google-cloud-storage
├── uv.lock                  # committed; reproducible builds
├── Procfile                 # web: python -m src.main  (buildpack entrypoint; key is "web" by convention)
└── README.md
```

**Package management: uv, natively supported.** Google's Python buildpack now resolves the package manager from project config: a committed `uv.lock` + `pyproject.toml` triggers uv during the Cloud Run source build (GA for Python 3.13+, Preview for ≤3.12 — pin Python ≥3.13 via `requires-python`). Do **not** keep a `requirements.txt` in the repo root: if present, it takes precedence over pyproject.toml. uv therefore costs nothing in deployment complexity and adds lockfile reproducibility that requirements.txt lacked.

Keep `main.py` a thin wrapper so the compute substrate can change (Jobs → Functions) without touching business logic. No Dockerfile — Buildpacks handles it.

## 11. Infrastructure as code & deployment runbook

### Required inputs (everything else is created by the stack)

| input | value | notes |
|---|---|---|
| `project_id` | provided by maintainer | the only required fact about the existing project |
| `region` | `us-central1` | a choice, not a lookup |
| `bucket` | `stmsn-bronze` | **pre-existing** — reference via a Terraform `data "google_storage_bucket"` source, never a managed `resource`; this stack must not own the bronze bucket's lifecycle |
| Routes API key | injected once via `gcloud secrets versions add` | never in repo, Terraform files, or state; restrict the key to the Routes API in the Maps console |

Preconditions to confirm (not provide): billing is linked to the project (required by the Routes API and Cloud Build), and the maintainer is authenticated locally (`gcloud auth login` and `gcloud auth application-default login` for Terraform).

Explicitly **not** pre-created: service accounts, the secret container, the scheduler job, API enablement — all Terraform resources. SA emails are derived from names (`route-traffic-runner@<project_id>.iam.gserviceaccount.com`); do not ask the maintainer for them.

### Split of responsibilities — Terraform owns everything around the job; gcloud owns the job:

| concern | tool | why |
|---|---|---|
| API enablement, service accounts, IAM bindings, secret *container*, Cloud Scheduler jobs | Terraform (`infra/`) | declarative, reviewable, reproducible |
| Cloud Run Job container (code + config) | `gcloud run jobs deploy --source` | source deploys are a gcloud/Buildpacks feature; Terraform would require managing an image URI and a build pipeline, fighting the ease-of-deployment priority |
| Secret *value* | `gcloud secrets versions add` (once, manual) | Terraform state stores secret versions in plaintext; keep the key out of state |

**Schedules: Terraform reads routes.yaml directly** — this replaces any sync script and keeps the YAML as the single source of truth:

```hcl
locals {
  cfg = yamldecode(file("${path.module}/../config/routes.yaml"))
}

resource "google_cloud_scheduler_job" "poll" {
  name      = "route-traffic"
  schedule  = local.cfg.poll_window_cron
  time_zone = local.cfg.timezone
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project}/locations/${var.region}/jobs/route-traffic:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
    # No overrides body: that needs run.jobs.runWithOverrides. The job labels itself.
  }
}
```

**Ordering note:** the `run.invoker` IAM binding on the job (`google_cloud_run_v2_job_iam_member`) requires the job to exist, and the job is created by gcloud. **Decision: use the job-scoped binding.** This project is general-purpose and will accumulate other Cloud Run resources; a project-level `roles/run.invoker` grant would let this scheduler SA invoke every current *and future* service/job in the project, a standing grant that widens silently as the project grows. The job-scoped binding's only cost is a one-time bootstrap ordering: first `terraform apply` errors on that single resource (job doesn't exist yet) → `gcloud run jobs deploy` → `terraform apply` again, clean from then on. (Equivalent: `-target` everything except the binding on the first apply.)

**Runbook:**

```bash
PROJECT=...; REGION=us-central1

# one-time bootstrap
cd infra && terraform init && terraform apply        # services, SAs, IAM, secret, schedulers
printf '%s' "$ROUTES_API_KEY" | gcloud secrets versions add routes-api-key --data-file=-

# every code or config change — this is the whole story
gcloud run jobs deploy route-traffic \
  --source . --region $REGION \
  --service-account route-traffic-runner@$PROJECT.iam.gserviceaccount.com \
  --set-secrets ROUTES_API_KEY=routes-api-key:latest \
  --set-env-vars BUCKET=stmsn-bronze,PREFIX=route-traffic/madison \
  --max-retries 1 --task-timeout 5m
# (wrapped by scripts/deploy.sh)

# schedule changes in routes.yaml
cd infra && terraform apply

# smoke test
gcloud run jobs execute route-traffic --region $REGION --wait
```

Terraform state: a GCS backend bucket (`terraform { backend "gcs" { ... } }`) is the natural choice given the stack; local state is acceptable for a single maintainer but the GCS backend costs nothing extra to set up.

## 12. Failure handling & observability

- Job sets `--max-retries 1`; per-route HTTP retries handled in-process (§5). Partial failure = warn and continue; total failure = nonzero exit → execution marked failed.
- All logs go to Cloud Logging automatically. v1 alerting: a log-based alert on `severity>=ERROR AND resource.type="cloud_run_job" AND resource.labels.job_name="route-traffic"` → email. Sufficient for a project of this size.
- Gap detection (did a scheduled run silently not happen?) is best done downstream: a dbt test or Dagster asset check asserting expected sample counts per day. Cheaper and more reliable than building heartbeat infrastructure into the poller.

## 13. Future extensions (out of scope for v1)

- **Dagster integration:** declare `route_traffic_bronze` as an external asset / source with a sensor or freshness check on the GCS prefix; dbt staging model normalizes to silver (delay seconds, local time, weekday flags — i.e., everything the R script's "cleaning" section did).
- **DuckDB consumption:** `SELECT * FROM read_parquet('gs://stmsn-bronze/route-traffic/madison/**/*.parquet', hive_partitioning=true)` behind a dbt source.
- **Polyline use:** encoded polylines are retained per alternative; decode downstream (e.g., `polyline` pip package) if route-choice analysis or mapping is wanted.
- **Compaction:** if years of 15-min samples make file counts annoying, add a monthly compaction step (DuckDB `COPY ... PARTITION_BY`) — not needed at this scale for a long time.