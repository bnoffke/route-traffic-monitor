from datetime import datetime, timedelta

from croniter import croniter

from .config import Schedule

# Cloud Scheduler fires on the minute, but queueing plus Cloud Run cold start can push
# container startup into the following minute. Without a backward tolerance that run would
# match no schedule and be silently dropped. Three minutes stays well inside the tightest slot
# spacing (10 minutes), so a run can never be attributed to the wrong schedule.
DEFAULT_TOLERANCE_MINUTES = 3


def active_schedule(
    schedules: list[Schedule],
    local_now: datetime,
    tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
) -> Schedule | None:
    """Return the schedule whose cron fired at (or just before) `local_now`, else None.

    `local_now` must already be in the config timezone — cron expressions are local-time.
    """
    minute = local_now.replace(second=0, microsecond=0)

    for offset in range(tolerance_minutes + 1):
        candidate = minute - timedelta(minutes=offset)
        for schedule in schedules:
            if croniter.match(schedule.cron, candidate):
                return schedule

    return None
