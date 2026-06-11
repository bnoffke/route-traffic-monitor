import io
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

PA_SCHEMA = pa.schema([
    pa.field("route_name", pa.string()),
    pa.field("corridor", pa.string()),
    pa.field("direction", pa.string()),
    pa.field("origin", pa.string()),
    pa.field("destination", pa.string()),
    pa.field("intermediate", pa.string()),
    pa.field("alternative_index", pa.int32()),
    pa.field("description", pa.string()),
    pa.field("distance_meters", pa.int32()),
    pa.field("duration_seconds", pa.int32()),
    pa.field("static_duration_seconds", pa.int32()),
    pa.field("encoded_polyline", pa.string()),
    pa.field("request_time_utc", pa.timestamp("us", tz="UTC")),
    pa.field("schedule_name", pa.string()),
    pa.field("run_id", pa.string()),
])


def write_parquet(
    records: list[dict],
    bucket_name: str,
    prefix: str,
    run_ts: datetime,
) -> str:
    dt_str = run_ts.strftime("%Y-%m-%d")
    run_ts_str = run_ts.strftime("%Y-%m-%dT%H%MZ")
    object_key = f"{prefix}/dt={dt_str}/run_ts={run_ts_str}.parquet"

    table = pa.Table.from_pylist(records, schema=PA_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    client = storage.Client()
    blob = client.bucket(bucket_name).blob(object_key)
    blob.upload_from_file(buf, content_type="application/octet-stream")

    return f"gs://{bucket_name}/{object_key}"
