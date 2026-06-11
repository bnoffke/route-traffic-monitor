import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import load_config
from .routes_client import poll_route
from .sink import write_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    config = load_config(Path(__file__).parent.parent / "config" / "routes.yaml")
    api_key = os.environ["ROUTES_API_KEY"]
    bucket = os.environ.get("BUCKET", "stmsn-bronze")
    prefix = os.environ.get("PREFIX", "route-traffic/madison")
    schedule_name = os.environ.get("SCHEDULE_NAME")
    run_id = os.environ.get("CLOUD_RUN_EXECUTION", "local")
    run_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    records: list[dict] = []
    failures = 0

    for route in config.routes:
        try:
            rows = poll_route(route, config.places, config.defaults, api_key, run_ts)
            for r in rows:
                r["run_id"] = run_id
                r["schedule_name"] = schedule_name
            records.extend(rows)
            log.info("Route %s: %d alternatives", route.name, len(rows))
        except Exception as exc:
            log.error("Route %s failed: %s", route.name, exc)
            failures += 1

    if failures == len(config.routes):
        log.error("All routes failed — exiting nonzero")
        sys.exit(1)

    gcs_path = write_parquet(records, bucket, prefix, run_ts)
    log.info("Wrote %d rows to %s", len(records), gcs_path)


if __name__ == "__main__":
    main()
