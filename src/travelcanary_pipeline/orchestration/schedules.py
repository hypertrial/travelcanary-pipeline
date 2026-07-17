from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from travelcanary_pipeline.orchestration.config import DAILY_SCHEDULE_ENABLED
from travelcanary_pipeline.orchestration.jobs import travelcanary_full_pipeline

_status = (
    DefaultScheduleStatus.RUNNING
    if DAILY_SCHEDULE_ENABLED
    else DefaultScheduleStatus.STOPPED
)

travelcanary_daily_schedule = ScheduleDefinition(
    job=travelcanary_full_pipeline,
    cron_schedule="0 8 * * *",
    default_status=_status,
)

__all__ = ["travelcanary_daily_schedule"]
