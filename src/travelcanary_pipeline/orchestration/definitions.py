from dagster import Definitions, in_process_executor
from dagster_dbt import DbtCliResource

from travelcanary_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)
from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.orchestration.assets import (
    ca_gac_raw_advisories,
    gdelt_raw_events,
    jp_mofa_raw_advisories,
    nl_mfa_raw_advisories,
    travelcanary_dbt,
    uk_fcdo_raw_advisories,
    us_state_raw_advisories,
)
from travelcanary_pipeline.orchestration.jobs import (
    travelcanary_dbt_build,
    travelcanary_full_pipeline,
    travelcanary_gdelt_ingest,
    travelcanary_official_advisories_ingest,
)
from travelcanary_pipeline.orchestration.schedules import travelcanary_daily_schedule

_SOURCE_CONTRACTS = load_source_contracts()

defs = Definitions(
    assets=[
        us_state_raw_advisories,
        ca_gac_raw_advisories,
        uk_fcdo_raw_advisories,
        nl_mfa_raw_advisories,
        jp_mofa_raw_advisories,
        gdelt_raw_events,
        travelcanary_dbt,
    ],
    jobs=[
        travelcanary_official_advisories_ingest,
        travelcanary_gdelt_ingest,
        travelcanary_dbt_build,
        travelcanary_full_pipeline,
    ],
    schedules=[travelcanary_daily_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
            profile="travelcanary",
            target="dev",
            dbt_executable=resolve_dbt_executable(),
        ),
    },
    executor=in_process_executor,
)
