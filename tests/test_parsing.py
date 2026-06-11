import io
import textwrap
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest
import yaml
from pydantic import ValidationError

from src.config import AppConfig, load_config
from src.routes_client import _parse_duration, poll_route
from src.sink import PA_SCHEMA, write_parquet

VALID_YAML = textwrap.dedent("""\
    defaults:
      travel_mode: DRIVE
      routing_preference: TRAFFIC_AWARE
      compute_alternative_routes: true
      language_code: en-US
      units: METRIC
    places:
      place_a: "ChIJaaa"
      place_b: "ChIJbbb"
    routes:
      - name: route_ab
        corridor: "Test Corridor"
        direction: NB
        origin: place_a
        destination: place_b
    schedules:
      - name: am-peak
        cron: "*/10 6-9 * * 1-5"
    timezone: America/Chicago
""")

API_RESPONSE = {
    "routes": [
        {
            "description": "via Main St",
            "distanceMeters": 3200,
            "duration": "420s",
            "staticDuration": "360s",
            "polyline": {"encodedPolyline": "abc123"},
        },
        {
            "description": "via Oak Ave",
            "distanceMeters": 3500,
            "duration": "480s",
            "staticDuration": "400s",
            "polyline": {"encodedPolyline": "def456"},
        },
    ]
}


# --- Config validation ---

def test_valid_config_loads():
    config = AppConfig.model_validate(yaml.safe_load(VALID_YAML))
    assert len(config.routes) == 1
    assert config.routes[0].name == "route_ab"


def test_unknown_origin_raises():
    data = yaml.safe_load(VALID_YAML)
    data["routes"][0]["origin"] = "nonexistent_place"
    with pytest.raises(ValidationError, match="not found in places"):
        AppConfig.model_validate(data)


def test_unknown_destination_raises():
    data = yaml.safe_load(VALID_YAML)
    data["routes"][0]["destination"] = "ghost"
    with pytest.raises(ValidationError, match="not found in places"):
        AppConfig.model_validate(data)


def test_duplicate_route_name_raises():
    data = yaml.safe_load(VALID_YAML)
    data["routes"].append(dict(data["routes"][0]))
    with pytest.raises(ValidationError, match="Duplicate route name"):
        AppConfig.model_validate(data)


def test_invalid_route_name_chars_raises():
    data = yaml.safe_load(VALID_YAML)
    data["routes"][0]["name"] = "Bad-Name!"
    with pytest.raises(ValidationError, match="must match"):
        AppConfig.model_validate(data)


# --- Duration parsing ---

def test_parse_duration_strips_s():
    assert _parse_duration("698s") == 698
    assert _parse_duration("0s") == 0
    assert _parse_duration("3600s") == 3600


# --- Response parsing via poll_route ---

def test_poll_route_returns_two_records(httpx_mock):
    httpx_mock.add_response(json=API_RESPONSE)

    config = AppConfig.model_validate(yaml.safe_load(VALID_YAML))
    run_ts = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)

    records = poll_route(
        config.routes[0],
        config.places,
        config.defaults,
        api_key="test-key",
        request_time=run_ts,
    )

    assert len(records) == 2
    assert records[0]["alternative_index"] == 0
    assert records[1]["alternative_index"] == 1
    assert records[0]["duration_seconds"] == 420
    assert records[0]["static_duration_seconds"] == 360
    assert records[0]["distance_meters"] == 3200
    assert records[0]["encoded_polyline"] == "abc123"
    assert records[0]["route_name"] == "route_ab"
    assert records[0]["corridor"] == "Test Corridor"
    assert records[0]["direction"] == "NB"
    assert records[0]["request_time_utc"] == run_ts


# --- Parquet round-trip ---

def test_parquet_schema_and_rowcount(tmp_path):
    run_ts = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)
    records = [
        {
            "route_name": "route_ab",
            "corridor": "Test",
            "direction": "NB",
            "origin": "place_a",
            "destination": "place_b",
            "intermediate": None,
            "alternative_index": 0,
            "description": "via Main",
            "distance_meters": 3200,
            "duration_seconds": 420,
            "static_duration_seconds": 360,
            "encoded_polyline": "abc123",
            "request_time_utc": run_ts,
            "schedule_name": None,
            "run_id": "test-run",
        }
    ]

    bucket_name = "test-bucket"
    prefix = "route-traffic/madison"

    captured = {}

    def fake_upload(buf, content_type=None):
        captured["data"] = buf.read()

    mock_blob = MagicMock()
    mock_blob.upload_from_file.side_effect = fake_upload
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    with patch("src.sink.storage.Client", return_value=mock_client):
        write_parquet(records, bucket_name, prefix, run_ts)

    table = pq.read_table(io.BytesIO(captured["data"]))
    assert table.num_rows == 1
    assert table.schema.equals(PA_SCHEMA)
