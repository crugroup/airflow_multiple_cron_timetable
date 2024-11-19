# airflow_multiple_cron_timetable
Airflow plugin for combining multiple cron schedules together.

## Timetables
### MultipleCronTimetable
This timetable takes a list of cron schedule strings and combines them together, such that the DAG will trigger when the next time for any of them occurs.

E.g. combining `0 0 * * *` and `0 6 * * *` would create a timetable that triggers at 00:00 and 06:00 every day.

It accepts the following parameters:

    :param schedules: List of cron schedule strings
    :param timezone: Timezone (default UTC)
