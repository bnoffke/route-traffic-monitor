from datetime import datetime, timedelta
from pathlib import Path

import pytest
from croniter import croniter

from src.config import load_config
from src.schedule import active_schedule

CONFIG_PATH = Path(__file__).parent.parent / "config" / "routes.yaml"

# 2024-01-01 is a Monday, so 2024-01-03 is a Wednesday, 01-06 Saturday, 01-07 Sunday.
WED = "2024-01-03"
SAT = "2024-01-06"
SUN = "2024-01-07"


@pytest.fixture(scope="module")
def config():
    return load_config(CONFIG_PATH)


def _at(day: str, hhmmss: str) -> datetime:
    return datetime.fromisoformat(f"{day}T{hhmmss}")


@pytest.mark.parametrize(
    "day,time,expected",
    [
        (WED, "07:20:00", "am-peak"),
        (WED, "16:30:00", "pm-peak"),
        (WED, "12:00:00", "midday-baseline"),
        (WED, "19:00:00", "pm-shoulder"),
        (SAT, "12:00:00", "weekend-baseline"),
        (SUN, "17:00:00", "weekend-baseline"),
    ],
)
def test_real_slots_match(config, day, time, expected):
    matched = active_schedule(config.schedules, _at(day, time))
    assert matched is not None
    assert matched.name == expected


@pytest.mark.parametrize(
    "day,time",
    [
        # Every one of these is a poll_window_cron fire that is NOT a real slot — the
        # skips are what let the five scheduler jobs collapse into one.
        (SAT, "07:20:00"),
        (SUN, "16:30:00"),
        (WED, "12:10:00"),
        (WED, "19:10:00"),
        (SAT, "19:00:00"),
    ],
)
def test_superset_fires_outside_slots_are_skipped(config, day, time):
    assert active_schedule(config.schedules, _at(day, time)) is None


@pytest.mark.parametrize("time", ["07:21:30", "07:23:00", "07:23:59"])
def test_late_start_within_tolerance_still_matches(config, time):
    # Scheduler fired at 07:20; queueing plus cold start delayed the container.
    matched = active_schedule(config.schedules, _at(WED, time))
    assert matched is not None
    assert matched.name == "am-peak"


def test_start_beyond_tolerance_does_not_match(config):
    assert active_schedule(config.schedules, _at(WED, "07:24:00")) is None


def test_tolerance_never_crosses_into_an_adjacent_slot(config):
    # pm-peak's last fire is 18:50 and pm-shoulder is 19:00 — the two nearest distinct
    # schedules. A late start must still be attributed to the slot that actually fired.
    assert active_schedule(config.schedules, _at(WED, "18:52:00")).name == "pm-peak"
    assert active_schedule(config.schedules, _at(WED, "19:02:00")).name == "pm-shoulder"


def test_poll_window_covers_every_slot(config):
    """Exhaustive proof that the single job polls exactly the old five jobs' times."""
    start = datetime(2024, 1, 1)
    wanted, actual = set(), set()

    for minute in range(7 * 24 * 60):
        t = start + timedelta(minutes=minute)
        if any(croniter.match(s.cron, t) for s in config.schedules):
            wanted.add(t)
        if croniter.match(config.poll_window_cron, t) and active_schedule(
            config.schedules, t, tolerance_minutes=0
        ):
            actual.add(t)

    assert wanted == actual
    assert len(wanted) == 5 * (4 * 6 + 4 * 6 + 2) + 2 * 2
