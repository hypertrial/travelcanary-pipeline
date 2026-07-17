from __future__ import annotations

from dagster import Config
from pydantic import Field, model_validator

from travelcanary_pipeline.config.settings import TRAVELCANARY_DAILY_SCHEDULE_ENABLED


class DbtBuildConfig(Config):
    full_refresh: bool = False
    progress_log_interval_seconds: int = Field(default=30, ge=1)
    progress_log_interval_events: int = Field(default=25, ge=1)
    no_progress_soft_timeout_seconds: int = Field(default=120, ge=1)
    no_progress_hard_timeout_seconds: int = Field(default=600, ge=1)
    progress_poll_seconds: int = Field(default=5, ge=1)

    @model_validator(mode="after")
    def _validate_timeouts(self) -> "DbtBuildConfig":
        if (
            self.no_progress_hard_timeout_seconds
            <= self.no_progress_soft_timeout_seconds
        ):
            raise ValueError(
                "no_progress_hard_timeout_seconds must be greater than "
                "no_progress_soft_timeout_seconds"
            )
        return self


DAILY_SCHEDULE_ENABLED = TRAVELCANARY_DAILY_SCHEDULE_ENABLED

__all__ = ["DAILY_SCHEDULE_ENABLED", "DbtBuildConfig"]
