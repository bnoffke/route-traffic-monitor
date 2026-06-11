"""Live API smoke test — skipped unless ROUTES_API_KEY is set in env or .env."""
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

from src.config import load_config
from src.routes_client import poll_route

CONFIG_PATH = Path(__file__).parent.parent / "config" / "routes.yaml"


@pytest.mark.skipif(not os.getenv("ROUTES_API_KEY"), reason="ROUTES_API_KEY not set")
def test_midvale_sb_live():
    config = load_config(CONFIG_PATH)
    route = next(r for r in config.routes if r.name == "midvale_sb")
    run_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    records = poll_route(
        route,
        config.places,
        config.defaults,
        api_key=os.environ["ROUTES_API_KEY"],
        request_time=run_ts,
    )

    assert len(records) >= 1, "Expected at least one route alternative"
    for rec in records:
        assert rec["duration_seconds"] > 0, "duration_seconds must be positive"
        assert rec["distance_meters"] > 0, "distance_meters must be positive"
        assert rec["encoded_polyline"], "encoded_polyline must not be empty"
        assert rec["route_name"] == "midvale_sb"
        assert rec["direction"] == "SB"


@pytest.mark.skipif(not os.getenv("ROUTES_API_KEY"), reason="ROUTES_API_KEY not set")
def test_midvale_nb_live():
    config = load_config(CONFIG_PATH)
    route = next(r for r in config.routes if r.name == "midvale_nb")
    run_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    records = poll_route(
        route,
        config.places,
        config.defaults,
        api_key=os.environ["ROUTES_API_KEY"],
        request_time=run_ts,
    )

    assert len(records) >= 1
    for rec in records:
        assert rec["duration_seconds"] > 0
        assert rec["distance_meters"] > 0
        assert rec["encoded_polyline"]
        assert rec["route_name"] == "midvale_nb"
        assert rec["direction"] == "NB"
