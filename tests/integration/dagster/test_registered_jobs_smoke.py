import shutil

import pytest
import yaml
from dagster import AssetKey, DagsterInstance

from travelcanary_pipeline.config.settings import BASE_DIR
from travelcanary_pipeline.orchestration.definitions import defs


@pytest.mark.integration
def test_definitions_register_assets_jobs_and_schedule():
    assert len(defs.assets) >= 6
    assert {job.name for job in defs.resolve_all_job_defs()} >= {
        "travelcanary_official_advisories_ingest",
        "travelcanary_gdelt_ingest",
        "travelcanary_dbt_build",
        "travelcanary_full_pipeline",
    }
    assert len(defs.schedules) == 1
    assert {job.executor_def.name for job in defs.resolve_all_job_defs()} == {
        "in_process"
    }

    instance_config = yaml.safe_load((BASE_DIR / "dagster_instance.yaml").read_text())
    assert instance_config["run_coordinator"]["config"]["max_concurrent_runs"] == 1


@pytest.mark.integration
def test_local_instance_loads_with_one_active_run(tmp_path):
    shutil.copy(BASE_DIR / "dagster_instance.yaml", tmp_path / "dagster.yaml")
    instance = DagsterInstance.from_config(str(tmp_path))
    try:
        assert instance.run_coordinator.get_run_queue_config().max_concurrent_runs == 1
    finally:
        instance.dispose()


@pytest.mark.integration
def test_dbt_sources_share_the_ingestion_asset_keys():
    graph = defs.resolve_asset_graph()
    expected_raw_keys = {
        AssetKey(["us_state", "raw", "advisories"]),
        AssetKey(["ca_gac", "raw", "advisories"]),
        AssetKey(["uk_fcdo", "raw", "advisories"]),
        AssetKey(["nl_mfa", "raw", "advisories"]),
        AssetKey(["jp_mofa", "raw", "advisories"]),
        AssetKey(["gdelt", "raw", "events"]),
    }
    assert expected_raw_keys <= graph.get_all_asset_keys()
    assert AssetKey(["us_state_raw", "advisories"]) not in graph.get_all_asset_keys()

    source_to_staging = {
        AssetKey(["us_state", "raw", "advisories"]): AssetKey(
            ["travelcanary_staging", "stg_us_state_advisories"]
        ),
        AssetKey(["gdelt", "raw", "events"]): AssetKey(
            ["travelcanary_staging", "stg_gdelt_events"]
        ),
    }
    for source_key, staging_key in source_to_staging.items():
        assert source_key in graph.get(staging_key).parent_keys
