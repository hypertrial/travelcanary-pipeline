"""Exact-output regression coverage for critical public marts."""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
GOLDEN_ROOT = REPO_ROOT / "tests/fixtures/golden"
MART_ORDER = {
    "country_travel_risk": ("destination_iso3, issuing_government, snapshot_date"),
    "country_risk_overview": "destination_iso3, snapshot_date",
    "country_advisory_changes": ("destination_iso3, issuing_government, snapshot_date"),
    "country_context_alerts": "destination_iso3, snapshot_date, alert_type",
    "source_data_quality": "source",
}


def _normalize(column: str, value: object) -> str:
    if value is None:
        return "<NULL>"
    if column == "hours_since_latest_run":
        return "<DYNAMIC>"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, datetime):
        return "<TIMESTAMP>"
    if isinstance(value, date):
        return "<DATE>"
    return str(value)


def _rows(
    conn: duckdb.DuckDBPyConnection, mart: str
) -> tuple[list[str], list[dict[str, str]]]:
    cursor = conn.execute(
        f"select * from travelcanary_marts.{mart} order by {MART_ORDER[mart]}"
    )
    columns = [item[0] for item in cursor.description]
    rows = [
        {
            column: _normalize(column, value)
            for column, value in zip(columns, row, strict=True)
        }
        for row in cursor.fetchall()
    ]
    return columns, rows


def _expected(mart: str) -> tuple[list[str], list[dict[str, str]]]:
    with (GOLDEN_ROOT / f"{mart}.csv").open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def _assert_exact(actual, expected) -> None:
    assert actual == expected


def _run(command: list[str], *, env: dict[str, str]) -> None:
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.integration
def test_critical_public_marts_match_reviewed_golden_rows(tmp_path, monkeypatch):
    warehouse = tmp_path / "golden.duckdb"
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        "travelcanary:\n"
        "  target: golden\n"
        "  outputs:\n"
        "    golden:\n"
        "      type: duckdb\n"
        f"      path: '{warehouse}'\n"
        "      schema: dbt\n"
        "      threads: 1\n"
    )
    monkeypatch.setenv("DUCKDB_PATH", str(warehouse))
    monkeypatch.setenv("DUCKDB_NAME", str(warehouse))
    env = os.environ.copy()

    _run([sys.executable, "scripts/seed_ci_warehouse.py"], env=env)
    _run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            str(profiles_dir),
            "--target-path",
            str(tmp_path / "dbt-target"),
        ],
        env=env,
    )

    conn = duckdb.connect(str(warehouse), read_only=True)
    try:
        for mart in MART_ORDER:
            actual_columns, actual_rows = _rows(conn, mart)
            expected_columns, expected_rows = _expected(mart)
            assert actual_columns == expected_columns
            _assert_exact(actual_rows, expected_rows)
    finally:
        conn.close()


def test_golden_comparison_rejects_changed_cell():
    with pytest.raises(AssertionError):
        _assert_exact([{"source": "changed"}], [{"source": "us_state"}])
