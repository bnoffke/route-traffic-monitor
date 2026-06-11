import logging
from datetime import datetime

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from .config import Defaults, Place, RouteEntry

ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
FIELD_MASK = (
    "routes.duration,"
    "routes.description,"
    "routes.staticDuration,"
    "routes.distanceMeters,"
    "routes.polyline.encodedPolyline"
)

log = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return isinstance(exc, (httpx.TimeoutException, httpx.TransportError))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
def _post(client: httpx.Client, api_key: str, body: dict) -> dict:
    response = client.post(
        ROUTES_URL,
        json=body,
        headers={
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        },
    )
    response.raise_for_status()
    return response.json()


def _parse_duration(value: str) -> int:
    return int(value.rstrip("s"))


def _waypoint(place: Place) -> dict:
    location: dict = {"latLng": {"latitude": place.lat, "longitude": place.lng}}
    if place.heading is not None:
        location["heading"] = place.heading
    return {"location": location}


def poll_route(
    route: RouteEntry,
    places: dict[str, Place],
    defaults: Defaults,
    api_key: str,
    request_time: datetime,
) -> list[dict]:
    body: dict = {
        "origin": _waypoint(places[route.origin]),
        "destination": _waypoint(places[route.destination]),
        "travelMode": defaults.travel_mode,
        "routingPreference": defaults.routing_preference,
        "computeAlternativeRoutes": defaults.compute_alternative_routes,
        "languageCode": defaults.language_code,
        "units": defaults.units,
    }
    if route.intermediate is not None:
        body["intermediates"] = [_waypoint(places[route.intermediate])]

    with httpx.Client(timeout=30) as client:
        data = _post(client, api_key, body)

    records = []
    for idx, api_route in enumerate(data.get("routes", [])):
        records.append({
            "route_name": route.name,
            "corridor": route.corridor,
            "direction": route.direction,
            "origin": route.origin,
            "destination": route.destination,
            "intermediate": route.intermediate,
            "alternative_index": idx,
            "description": api_route.get("description", ""),
            "distance_meters": api_route.get("distanceMeters", 0),
            "duration_seconds": _parse_duration(api_route["duration"]),
            "static_duration_seconds": _parse_duration(api_route["staticDuration"]),
            "encoded_polyline": api_route.get("polyline", {}).get("encodedPolyline", ""),
            "request_time_utc": request_time,
        })

    if route.expected_distance_m and records:
        exp = route.expected_distance_m
        primary = records[0]["distance_meters"]
        if abs(primary - exp) / exp > 0.25:
            log.warning(
                "Route %s primary distance %dm deviates >25%% from expected %dm "
                "(possible wrong-carriageway snap)",
                route.name, primary, exp,
            )

    return records
