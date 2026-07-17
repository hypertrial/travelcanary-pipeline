"""Central settings barrel."""

from __future__ import annotations

import travelcanary_pipeline.config.settings_ingestion as _settings_ingestion
import travelcanary_pipeline.config.settings_warehouse as _settings_warehouse
from travelcanary_pipeline.config._env import (  # noqa: F401
    _env_bool,
    _env_int,
    _optional_env_str,
)
from travelcanary_pipeline.config.settings_ingestion import *  # noqa: F403
from travelcanary_pipeline.config.settings_warehouse import *  # noqa: F403

__all__ = list(
    dict.fromkeys(
        (
            "_env_bool",
            "_env_int",
            "_optional_env_str",
            *_settings_warehouse.__all__,
            *_settings_ingestion.__all__,
        )
    )
)
