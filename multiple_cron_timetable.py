from airflow.plugins_manager import AirflowPlugin

from datetime import datetime

import pendulum
from croniter import croniter

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class MultipleCronTimetable(Timetable):
    def __init__(self, schedules: list[str] = None, timezone: str = "UTC"):
        self.schedules = schedules if schedules is not None else []
        self.timezone = timezone

    def infer_manual_data_interval(self, run_after: datetime) -> DataInterval:
        tz = pendulum.timezone(self.timezone)
        start = min(croniter(cron, run_after).get_prev(datetime) for cron in self.schedules).astimezone(tz)
        end = min(croniter(cron, run_after).get_next(datetime) for cron in self.schedules).astimezone(tz)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: pendulum.Interval,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        tz = pendulum.timezone(self.timezone)
        if not restriction.catchup:
            last_start = pendulum.now(tz)
        elif last_automated_data_interval is None:  # This is the first run.
            last_start = restriction.earliest
            if last_start is None:
                return None  # No start date specified; do not run.
        else:
            last_start = last_automated_data_interval.start
        next_starts = [croniter(cron, last_start).get_next(datetime).astimezone(tz) for cron in self.schedules]
        next_start = min(next_starts)
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # The next start is later than the restriction's latest.
        next_end = next_start  # Each run is instantaneous.
        if not isinstance(next_start, datetime) or not isinstance(next_end, datetime):
            raise TypeError(f"Expected datetime instances, but got {type(next_start)} and {type(next_end)}")
        return DagRunInfo.interval(start=next_start, end=next_end)

    @classmethod
    def deserialize(cls, serialized: dict) -> "MultipleCronTimetable":
        return cls(schedules=serialized["schedules"], timezone=serialized.get("timezone", "UTC"))

    def serialize(self) -> dict:
        return {"schedules": self.schedules, "timezone": self.timezone}


class MultipleCronTimetablePlugin(AirflowPlugin):
    name = "multiple_cron_timetable"
    timetables = [MultipleCronTimetable]
