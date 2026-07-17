from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)
from dagster_dbt import DbtCliResource, dbt_assets

from travelcanary_pipeline.config.settings import UK_FCDO_DISCOVER_ALL_COUNTRIES
from travelcanary_pipeline.ingestion.ca_gac.advisories import sync_ca_gac_advisories
from travelcanary_pipeline.ingestion.gdelt.events import sync_gdelt_daily_events
from travelcanary_pipeline.ingestion.jp_mofa.advisories import sync_jp_mofa_advisories
from travelcanary_pipeline.ingestion.nl_mfa.advisories import sync_nl_mfa_advisories
from travelcanary_pipeline.ingestion.uk_fcdo.advisories import sync_uk_fcdo_advisories
from travelcanary_pipeline.ingestion.us_state.advisories import sync_us_state_advisories
from travelcanary_pipeline.naming import (
    SOURCE_CA_GAC,
    SOURCE_GDELT,
    SOURCE_JP_MOFA,
    SOURCE_NL_MFA,
    SOURCE_UK_FCDO,
    SOURCE_US_STATE,
    asset_key,
)
from travelcanary_pipeline.orchestration.config import DbtBuildConfig
from travelcanary_pipeline.orchestration.dbt_build import stream_dbt_build
from travelcanary_pipeline.orchestration.dbt_project import DBT_MANIFEST_PATH


@multi_asset(
    name="us_state_raw_advisories",
    specs=[AssetSpec(key=asset_key(SOURCE_US_STATE, "raw", "advisories"), deps=[])],
    group_name="ingestion",
)
def us_state_raw_advisories(context: AssetExecutionContext) -> MaterializeResult:
    summary = sync_us_state_advisories()
    context.log.info("us_state sync: %s", summary)
    return MaterializeResult(metadata={"rows": MetadataValue.int(int(summary["rows"]))})


@multi_asset(
    name="ca_gac_raw_advisories",
    specs=[AssetSpec(key=asset_key(SOURCE_CA_GAC, "raw", "advisories"), deps=[])],
    group_name="ingestion",
)
def ca_gac_raw_advisories(context: AssetExecutionContext) -> MaterializeResult:
    summary = sync_ca_gac_advisories()
    context.log.info("ca_gac sync: %s", summary)
    return MaterializeResult(metadata={"rows": MetadataValue.int(int(summary["rows"]))})


@multi_asset(
    name="uk_fcdo_raw_advisories",
    specs=[AssetSpec(key=asset_key(SOURCE_UK_FCDO, "raw", "advisories"), deps=[])],
    group_name="ingestion",
)
def uk_fcdo_raw_advisories(context: AssetExecutionContext) -> MaterializeResult:
    summary = sync_uk_fcdo_advisories(discover_all=UK_FCDO_DISCOVER_ALL_COUNTRIES)
    context.log.info("uk_fcdo sync: %s", summary)
    return MaterializeResult(metadata={"rows": MetadataValue.int(int(summary["rows"]))})


@multi_asset(
    name="nl_mfa_raw_advisories",
    specs=[AssetSpec(key=asset_key(SOURCE_NL_MFA, "raw", "advisories"), deps=[])],
    group_name="ingestion",
)
def nl_mfa_raw_advisories(context: AssetExecutionContext) -> MaterializeResult:
    summary = sync_nl_mfa_advisories()
    context.log.info("nl_mfa sync: %s", summary)
    return MaterializeResult(metadata={"rows": MetadataValue.int(int(summary["rows"]))})


@multi_asset(
    name="jp_mofa_raw_advisories",
    specs=[AssetSpec(key=asset_key(SOURCE_JP_MOFA, "raw", "advisories"), deps=[])],
    group_name="ingestion",
)
def jp_mofa_raw_advisories(context: AssetExecutionContext) -> MaterializeResult:
    summary = sync_jp_mofa_advisories()
    context.log.info("jp_mofa sync: %s", summary)
    return MaterializeResult(metadata={"rows": MetadataValue.int(int(summary["rows"]))})


@multi_asset(
    name="gdelt_raw_events",
    specs=[AssetSpec(key=asset_key(SOURCE_GDELT, "raw", "events"), deps=[])],
    group_name="ingestion",
)
def gdelt_raw_events(context: AssetExecutionContext) -> MaterializeResult:
    summary = sync_gdelt_daily_events()
    context.log.info("gdelt sync: %s", summary)
    return MaterializeResult(metadata={"rows": MetadataValue.int(int(summary["rows"]))})


@dbt_assets(manifest=DBT_MANIFEST_PATH)
def travelcanary_dbt(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtBuildConfig,
):
    yield from stream_dbt_build(
        asset_name="travelcanary_dbt",
        context=context,
        dbt=dbt,
        config=config,
    )


__all__ = [
    "ca_gac_raw_advisories",
    "gdelt_raw_events",
    "jp_mofa_raw_advisories",
    "nl_mfa_raw_advisories",
    "travelcanary_dbt",
    "uk_fcdo_raw_advisories",
    "us_state_raw_advisories",
]
