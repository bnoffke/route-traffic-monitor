"""One-off backfill: move parquet files whose dt= partition was computed from the
UTC date into the correct local-date partition.

Usage:
    python scripts/backfill_dt_partitions.py            # dry run (default)
    python scripts/backfill_dt_partitions.py --apply    # actually move objects
"""

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from google.cloud import storage

KEY_RE = re.compile(r"^(?P<prefix>.*)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/run_ts=(?P<ts>\d{4}-\d{2}-\d{2}T\d{4})Z\.parquet$")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the moves (default is dry run)")
    parser.add_argument("--bucket", default=os.environ.get("BUCKET", "stmsn-bronze"))
    parser.add_argument("--prefix", default=os.environ.get("PREFIX", "route-traffic/madison"))
    parser.add_argument("--timezone", default="America/Chicago")
    args = parser.parse_args()

    tz = ZoneInfo(args.timezone)
    client = storage.Client()
    bucket = client.bucket(args.bucket)

    moves = []
    for blob in client.list_blobs(args.bucket, prefix=f"{args.prefix}/dt="):
        m = KEY_RE.match(blob.name)
        if not m:
            print(f"skip (unrecognized key): {blob.name}", file=sys.stderr)
            continue
        run_ts = datetime.strptime(m["ts"], "%Y-%m-%dT%H%M").replace(tzinfo=timezone.utc)
        correct_dt = run_ts.astimezone(tz).strftime("%Y-%m-%d")
        if correct_dt != m["dt"]:
            new_key = f"{m['prefix']}/dt={correct_dt}/run_ts={m['ts']}Z.parquet"
            moves.append((blob, new_key))

    if not moves:
        print("Nothing to move — all partitions already correct.")
        return

    for blob, new_key in moves:
        if args.apply:
            bucket.rename_blob(blob, new_key)
            print(f"moved: {blob.name} -> {new_key}")
        else:
            print(f"would move: {blob.name} -> {new_key}")

    if not args.apply:
        print(f"\nDry run: {len(moves)} object(s) would be moved. Re-run with --apply to execute.")


if __name__ == "__main__":
    main()
