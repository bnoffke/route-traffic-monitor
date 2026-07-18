import io
import json
import logging
import textwrap
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest
import yaml
from pydantic import ValidationError

from src.config import AppConfig, Place, load_config
from src.routes_client import _parse_duration, _waypoint, poll_route
from src.sink import PA_SCHEMA, write_parquet

VALID_YAML = textwrap.dedent("""\
    defaults:
      travel_mode: DRIVE
      routing_preference: TRAFFIC_AWARE
      compute_alternative_routes: true
      language_code: en-US
      units: METRIC
    places:
      place_a: { lat: 43.07, lng: -89.45 }
      place_b: { lat: 43.06, lng: -89.45 }
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


# --- Waypoint body shape (lat/lng) ---

def test_waypoint_body_shape(httpx_mock):
    httpx_mock.add_response(json=API_RESPONSE)

    config = AppConfig.model_validate(yaml.safe_load(VALID_YAML))
    run_ts = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)

    poll_route(
        config.routes[0],
        config.places,
        config.defaults,
        api_key="test-key",
        request_time=run_ts,
    )

    body = json.loads(httpx_mock.get_requests()[0].content)
    assert body["origin"] == {
        "location": {"latLng": {"latitude": 43.07, "longitude": -89.45}}
    }
    assert body["destination"] == {
        "location": {"latLng": {"latitude": 43.06, "longitude": -89.45}}
    }
    # heading absent when not configured
    assert "heading" not in body["origin"]["location"]
    # no placeId anywhere
    assert "placeId" not in body["origin"]


def test_waypoint_includes_heading():
    wp = _waypoint(Place(lat=43.07, lng=-89.45, heading=350))
    assert wp == {
        "location": {
            "latLng": {"latitude": 43.07, "longitude": -89.45},
            "heading": 350,
        }
    }


# --- expected_distance_m deviation WARN ---

def _config_with_expected(expected_m):
    data = yaml.safe_load(VALID_YAML)
    data["routes"][0]["expected_distance_m"] = expected_m
    return AppConfig.model_validate(data)


def test_expected_distance_warns(httpx_mock, caplog):
    httpx_mock.add_response(json=API_RESPONSE)
    # primary distance is 3200m; expect 1000m -> 220% deviation -> WARN
    config = _config_with_expected(1000)
    run_ts = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)

    with caplog.at_level(logging.WARNING):
        poll_route(config.routes[0], config.places, config.defaults,
                   api_key="test-key", request_time=run_ts)

    assert any("deviates >25%" in r.message for r in caplog.records)


def test_expected_distance_within_threshold(httpx_mock, caplog):
    httpx_mock.add_response(json=API_RESPONSE)
    # primary distance is 3200m; expect 3100m -> ~3% deviation -> no WARN
    config = _config_with_expected(3100)
    run_ts = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)

    with caplog.at_level(logging.WARNING):
        poll_route(config.routes[0], config.places, config.defaults,
                   api_key="test-key", request_time=run_ts)

    assert not any("deviates" in r.message for r in caplog.records)


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
        write_parquet(records, bucket_name, prefix, run_ts, "America/Chicago")

    table = pq.read_table(io.BytesIO(captured["data"]))
    assert table.num_rows == 1
    assert table.schema.equals(PA_SCHEMA)


def test_write_parquet_dt_partition_uses_local_date():
    def object_key_for(run_ts):
        mock_bucket = MagicMock()
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        with patch("src.sink.storage.Client", return_value=mock_client):
            write_parquet([], "test-bucket", "p", run_ts, "America/Chicago")
        return mock_bucket.blob.call_args[0][0]

    evening = datetime(2026, 7, 18, 0, 1, tzinfo=timezone.utc)
    assert object_key_for(evening) == "p/dt=2026-07-17/run_ts=2026-07-18T0001Z.parquet"

    midday = datetime(2026, 7, 18, 17, 2, tzinfo=timezone.utc)
    assert object_key_for(midday) == "p/dt=2026-07-18/run_ts=2026-07-18T1702Z.parquet"
