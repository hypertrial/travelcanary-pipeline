from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ["DBT_PROFILES_DIR"] = str(REPO_ROOT / "dbt" / "profiles")


@pytest.fixture(autouse=True)
def _isolate_duckdb(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    from travelcanary_pipeline.storage.duckdb.connection import (
        reset_duckdb_connection_state,
    )

    reset_duckdb_connection_state()
    yield
    reset_duckdb_connection_state()
