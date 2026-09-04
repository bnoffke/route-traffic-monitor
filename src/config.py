import re
from datetime import datetime
from pathlib import Path

import yaml
from croniter import croniter
from pydantic import BaseModel, Field, model_validator

# Arbitrary fixed week used to prove `poll_window_cron` covers every schedule.
# Starts on a Monday so all seven day-of-week values are exercised.
_COVERAGE_WEEK_START = datetime(2024, 1, 1)
_COVERAGE_WEEK_END = datetime(2024, 1, 8)


class Defaults(BaseModel):
    travel_mode: str = "DRIVE"
    routing_preference: str = "TRAFFIC_AWARE"
    compute_alternative_routes: bool = True
    language_code: str = "en-US"
    units: str = "METRIC"


class Place(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    heading: int | None = Field(default=None, ge=0, le=360)


class RouteEntry(BaseModel):
    name: str
    corridor: str
    direction: str
    origin: str
    destination: str
    intermediate: str | None = None
    expected_distance_m: int | None = None


class Schedule(BaseModel):
    name: str
    cron: str


class AppConfig(BaseModel):
    defaults: Defaults
    places: dict[str, Place]
    routes: list[RouteEntry]
    schedules: list[Schedule]
    timezone: str
    poll_window_cron: str

    @model_validator(mode="after")
    def _validate(self) -> "AppConfig":
        name_pattern = re.compile(r"^[a-z0-9_]+$")
        seen_names: set[str] = set()

        if not croniter.is_valid(self.poll_window_cron):
            raise ValueError(
                f"poll_window_cron '{self.poll_window_cron}' is not a valid cron expression"
            )

        for schedule in self.schedules:
            if not croniter.is_valid(schedule.cron):
                raise ValueError(
                    f"Schedule '{schedule.name}' cron '{schedule.cron}' "
                    "is not a valid cron expression"
                )

            # The single Cloud Scheduler job fires on poll_window_cron; a schedule slot it
            # does not cover would simply never be polled, so fail loudly at load time.
            it = croniter(schedule.cron, _COVERAGE_WEEK_START)
            fire = it.get_next(datetime)
            while fire < _COVERAGE_WEEK_END:
                if not croniter.match(self.poll_window_cron, fire):
                    raise ValueError(
                        f"poll_window_cron '{self.poll_window_cron}' does not cover "
                        f"schedule '{schedule.name}' ('{schedule.cron}'): "
                        f"no fire at {fire:%a %Y-%m-%d %H:%M}"
                    )
                fire = it.get_next(datetime)

        for route in self.routes:
            if not name_pattern.match(route.name):
                raise ValueError(
                    f"Route name '{route.name}' must match [a-z0-9_]+"
                )
            if route.name in seen_names:
                raise ValueError(f"Duplicate route name: '{route.name}'")
            seen_names.add(route.name)

            for field, val in [
                ("origin", route.origin),
                ("destination", route.destination),
                ("intermediate", route.intermediate),
            ]:
                if val is not None and val not in self.places:
                    raise ValueError(
                        f"Route '{route.name}' {field} '{val}' not found in places"
                    )

        return self


def load_config(path: Path | str) -> AppConfig:
    with open(path) as f:
        data = yaml.safe_load(f)
    return AppConfig.model_validate(data)
