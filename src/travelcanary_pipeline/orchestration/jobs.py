from __future__ import annotations

from dagster import AssetSelection, define_asset_job

from travelcanary_pipeline.orchestration.assets import (
    ca_gac_raw_advisories,
    gdelt_raw_events,
    jp_mofa_raw_advisories,
    nl_mfa_raw_advisories,
    travelcanary_dbt,
    uk_fcdo_raw_advisories,
    us_state_raw_advisories,
)

OFFICIAL_ASSETS = [
    us_state_raw_advisories,
    ca_gac_raw_advisories,
    uk_fcdo_raw_advisories,
    nl_mfa_raw_advisories,
    jp_mofa_raw_advisories,
]

travelcanary_official_advisories_ingest = define_asset_job(
    name="travelcanary_official_advisories_ingest",
    selection=AssetSelection.assets(*OFFICIAL_ASSETS),
)

travelcanary_gdelt_ingest = define_asset_job(
    name="travelcanary_gdelt_ingest",
    selection=AssetSelection.assets(gdelt_raw_events),
)

travelcanary_dbt_build = define_asset_job(
    name="travelcanary_dbt_build",
    selection=AssetSelection.assets(travelcanary_dbt),
)

travelcanary_full_pipeline = define_asset_job(
    name="travelcanary_full_pipeline",
    selection=AssetSelection.assets(
        *OFFICIAL_ASSETS,
        gdelt_raw_events,
        travelcanary_dbt,
    ),
)

__all__ = [
    "travelcanary_dbt_build",
    "travelcanary_full_pipeline",
    "travelcanary_gdelt_ingest",
    "travelcanary_official_advisories_ingest",
]
