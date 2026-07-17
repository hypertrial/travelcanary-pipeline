from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import materialize
from dagster_dbt import DbtCliResource

from travelcanary_pipeline.config.settings import resolve_dbt_executable
from travelcanary_pipeline.orchestration.assets import (
    ca_gac_raw_advisories,
    gdelt_raw_events,
    jp_mofa_raw_advisories,
    nl_mfa_raw_advisories,
    travelcanary_dbt,
    uk_fcdo_raw_advisories,
    us_state_raw_advisories,
)
from travelcanary_pipeline.storage.duckdb import connection


@pytest.fixture
def pipeline_duckdb(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "pipeline_smoke.duckdb"
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        """
travelcanary:
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH') }}"
      schema: dbt
      threads: 2
  target: dev
"""
    )
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
    connection.reset_duckdb_connection_state()
    yield profiles_dir
    connection.reset_duckdb_connection_state()


@pytest.mark.integration
def test_full_pipeline_materializes_with_mocked_ingest(pipeline_duckdb, monkeypatch):
    from scripts.seed_ci_warehouse import main as seed_ci_warehouse

    seed_ci_warehouse()

    def _noop_sync(source: str):
        def _sync(**_kwargs):
            return {"source": source, "rows": 1}

        return _sync

    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.assets.sync_us_state_advisories",
        _noop_sync("us_state"),
    )
    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.assets.sync_ca_gac_advisories",
        _noop_sync("ca_gac"),
    )
    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.assets.sync_uk_fcdo_advisories",
        _noop_sync("uk_fcdo"),
    )
    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.assets.sync_nl_mfa_advisories",
        _noop_sync("nl_mfa"),
    )
    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.assets.sync_jp_mofa_advisories",
        _noop_sync("jp_mofa"),
    )
    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.assets.sync_gdelt_daily_events",
        _noop_sync("gdelt"),
    )

    result = materialize(
        [
            us_state_raw_advisories,
            ca_gac_raw_advisories,
            uk_fcdo_raw_advisories,
            nl_mfa_raw_advisories,
            jp_mofa_raw_advisories,
            gdelt_raw_events,
            travelcanary_dbt,
        ],
        resources={
            "dbt": DbtCliResource(
                project_dir=str(Path("dbt").resolve()),
                profiles_dir=str(pipeline_duckdb.resolve()),
                profile="travelcanary",
                target="dev",
                dbt_executable=resolve_dbt_executable(),
            )
        },
    )
    assert result.success
