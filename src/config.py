import re
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator


class Defaults(BaseModel):
    travel_mode: str = "DRIVE"
    routing_preference: str = "TRAFFIC_AWARE"
    compute_alternative_routes: bool = True
    language_code: str = "en-US"
    units: str = "METRIC"


class RouteEntry(BaseModel):
    name: str
    corridor: str
    direction: str
    origin: str
    destination: str
    intermediate: str | None = None


class Schedule(BaseModel):
    name: str
    cron: str


class AppConfig(BaseModel):
    defaults: Defaults
    places: dict[str, str]
    routes: list[RouteEntry]
    schedules: list[Schedule]
    timezone: str

    @model_validator(mode="after")
    def _validate(self) -> "AppConfig":
        name_pattern = re.compile(r"^[a-z0-9_]+$")
        seen_names: set[str] = set()

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
