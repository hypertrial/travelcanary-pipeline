from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


@pytest.mark.integration
def test_failed_dbt_build_preserves_published_marts_and_new_raw_state(
    tmp_path: Path, monkeypatch
):
    warehouse = tmp_path / "atomic.duckdb"
    target = tmp_path / "dbt-target"
    monkeypatch.setenv("DUCKDB_PATH", str(warehouse))
    monkeypatch.setenv("DUCKDB_NAME", str(warehouse))
    monkeypatch.setenv("DBT_TARGET_PATH", str(target))
    monkeypatch.setenv("DBT_LOG_PATH", str(target))

    from scripts.run_atomic_dbt_build import main as run_dbt_build
    from scripts.seed_ci_warehouse import main as seed_warehouse

    from travelcanary_pipeline.storage.duckdb.atomic_build import _candidate_prefix
    from travelcanary_pipeline.storage.duckdb.connection import (
        reset_duckdb_connection_state,
    )

    reset_duckdb_connection_state()
    seed_warehouse()
    assert run_dbt_build() == 0

    conn = duckdb.connect(str(warehouse))
    try:
        before = conn.execute(
            "select * from travelcanary_marts.country_travel_risk "
            "order by destination_iso3, issuing_government"
        ).fetchall()
        conn.execute(
            "update us_state_raw.advisories set native_level = 'future-value' "
            "where destination_iso2 = 'TH'"
        )
    finally:
        conn.close()

    with pytest.raises(SystemExit):
        run_dbt_build()

    conn = duckdb.connect(str(warehouse), read_only=True)
    try:
        assert (
            conn.execute(
                "select * from travelcanary_marts.country_travel_risk "
                "order by destination_iso3, issuing_government"
            ).fetchall()
            == before
        )
        assert conn.execute(
            "select native_level from us_state_raw.advisories "
            "where destination_iso2 = 'TH'"
        ).fetchone() == ("future-value",)
    finally:
        conn.close()

    assert not list(tmp_path.glob(f"{_candidate_prefix(warehouse)}*"))
    reset_duckdb_connection_state()
