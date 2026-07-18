"""Integration coverage for public mart export and history transfer."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from travelcanary_pipeline.config import settings as settings_mod
from travelcanary_pipeline.export import export_public_marts
from travelcanary_pipeline.history_transfer import export_history, import_history
from travelcanary_pipeline.public_contracts import PUBLIC_MART_COLUMNS, PUBLIC_MARTS
from travelcanary_pipeline.storage.duckdb.connection import (
    get_persistent_connection,
    reset_duckdb_connection_state,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def _dbt_build() -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt/profiles",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def _seed_and_build() -> None:
    subprocess.run(
        [sys.executable, "scripts/seed_ci_warehouse.py"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    _dbt_build()


def _delete_warehouse() -> None:
    reset_duckdb_connection_state()
    warehouse_path = settings_mod.resolve_duckdb_path()
    for suffix in ("", ".wal", ".writer.lock"):
        candidate = Path(f"{warehouse_path}{suffix}")
        if candidate.exists():
            candidate.unlink()


@pytest.mark.integration
def test_export_public_marts_from_seeded_warehouse(tmp_path):
    _seed_and_build()
    output_dir = tmp_path / "mart-exports"
    manifest = export_public_marts(output_dir)
    assert set(manifest["marts"]) == set(PUBLIC_MARTS)

    verify = duckdb.connect()
    try:
        for mart in PUBLIC_MARTS:
            parquet_path = output_dir / f"{mart}.parquet"
            columns = [
                row[0]
                for row in verify.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)",
                    [str(parquet_path)],
                ).fetchall()
            ]
            assert columns == PUBLIC_MART_COLUMNS[mart]
            count = verify.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(parquet_path)],
            ).fetchone()[0]
            assert count == manifest["marts"][mart]["row_count"]
    finally:
        verify.close()

    loaded = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert (
        loaded["marts"][PUBLIC_MARTS[0]]["columns"]
        == PUBLIC_MART_COLUMNS[PUBLIC_MARTS[0]]
    )


@pytest.mark.integration
def test_history_export_import_survives_warehouse_rebuild(tmp_path):
    _seed_and_build()

    conn = get_persistent_connection()
    try:
        conn.execute(
            """
            UPDATE travelcanary_marts.country_travel_risk_history
            SET snapshot_date = current_date - INTERVAL 1 DAY
            """
        )
        conn.execute(
            """
            UPDATE us_state_raw.advisories
            SET native_level = '4', native_level_label = 'Do not travel'
            WHERE destination_iso2 = 'TH'
            """
        )
        conn.commit()
    finally:
        conn.close()

    _dbt_build()

    conn = get_persistent_connection()
    try:
        history_before = conn.execute(
            """
            SELECT destination_iso3, issuing_government, snapshot_date,
                   normalized_ordinal
            FROM travelcanary_marts.country_travel_risk_history
            ORDER BY 1, 2, 3
            """
        ).fetchall()
        prior_day_rows = conn.execute(
            """
            SELECT destination_iso3, issuing_government, snapshot_date,
                   normalized_ordinal
            FROM travelcanary_marts.country_travel_risk_history
            WHERE snapshot_date < current_date
            ORDER BY 1, 2, 3
            """
        ).fetchall()
        changes_before = conn.execute(
            """
            SELECT destination_iso3, issuing_government, advisory_direction
            FROM travelcanary_marts.country_advisory_changes
            WHERE destination_iso3 = 'THA' AND issuing_government = 'us_state'
            """
        ).fetchall()
        assert len(history_before) >= 2
        assert prior_day_rows
        assert changes_before
    finally:
        conn.close()

    history_path = tmp_path / "country_travel_risk_history.parquet"
    export_history(history_path)

    _delete_warehouse()
    _seed_and_build()
    import_history(history_path)
    _dbt_build()

    conn = get_persistent_connection()
    try:
        history_after = conn.execute(
            """
            SELECT destination_iso3, issuing_government, snapshot_date,
                   normalized_ordinal
            FROM travelcanary_marts.country_travel_risk_history
            ORDER BY 1, 2, 3
            """
        ).fetchall()
        assert set(prior_day_rows).issubset(set(history_after))
        changes_after = conn.execute(
            """
            SELECT destination_iso3, issuing_government, advisory_direction
            FROM travelcanary_marts.country_advisory_changes
            WHERE destination_iso3 = 'THA' AND issuing_government = 'us_state'
            """
        ).fetchall()
        assert changes_after
        trends = conn.execute(
            """
            SELECT COUNT(*)
            FROM travelcanary_marts.country_risk_trends
            WHERE destination_iso3 = 'THA'
            """
        ).fetchone()[0]
        assert trends >= 1
    finally:
        conn.close()
