"""Unit tests for public mart Parquet export."""

from __future__ import annotations

import importlib
import json

import duckdb
import pytest

from travelcanary_pipeline.export import ExportError, export_public_marts
from travelcanary_pipeline.public_contracts import PUBLIC_MART_COLUMNS, PUBLIC_MARTS
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection


def _seed_one_mart(conn: duckdb.DuckDBPyConnection, mart: str) -> None:
    columns = PUBLIC_MART_COLUMNS[mart]
    col_defs = ", ".join(f"{name} VARCHAR" for name in columns)
    conn.execute("CREATE SCHEMA IF NOT EXISTS travelcanary_marts")
    conn.execute(f"CREATE OR REPLACE TABLE travelcanary_marts.{mart} ({col_defs})")
    placeholders = ", ".join("?" for _ in columns)
    values = [f"{mart}-{idx}" for idx, _ in enumerate(columns)]
    conn.execute(
        f"INSERT INTO travelcanary_marts.{mart} VALUES ({placeholders})",
        values,
    )


def test_export_public_marts_writes_parquet_and_manifest(tmp_path):
    conn = get_persistent_connection()
    try:
        for mart in PUBLIC_MARTS:
            _seed_one_mart(conn, mart)
        conn.commit()
    finally:
        conn.close()

    output_dir = tmp_path / "exports"
    manifest = export_public_marts(output_dir)

    assert set(manifest["marts"]) == set(PUBLIC_MARTS)
    assert (output_dir / "manifest.json").is_file()
    loaded = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert loaded["package_version"]
    assert loaded["exported_at"].endswith("Z")

    verify = duckdb.connect()
    try:
        for mart in PUBLIC_MARTS:
            parquet_path = output_dir / f"{mart}.parquet"
            assert parquet_path.is_file()
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
            assert count == 1
            assert manifest["marts"][mart]["row_count"] == 1
    finally:
        verify.close()


def test_export_public_marts_rejects_missing_mart(tmp_path):
    conn = get_persistent_connection()
    try:
        for mart in PUBLIC_MARTS[:-1]:
            _seed_one_mart(conn, mart)
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ExportError, match="public mart is missing"):
        export_public_marts(tmp_path / "exports")


def test_export_public_marts_rejects_column_drift(tmp_path):
    conn = get_persistent_connection()
    try:
        for mart in PUBLIC_MARTS:
            _seed_one_mart(conn, mart)
        first = PUBLIC_MARTS[0]
        conn.execute(
            f"ALTER TABLE travelcanary_marts.{first} ADD COLUMN drift_column VARCHAR"
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ExportError, match="column contract mismatch"):
        export_public_marts(tmp_path / "exports")


def test_export_public_marts_cli_success_and_failure(tmp_path, monkeypatch):
    from scripts import export_public_marts as cli

    conn = get_persistent_connection()
    try:
        for mart in PUBLIC_MARTS:
            _seed_one_mart(conn, mart)
        conn.commit()
    finally:
        conn.close()

    output_dir = tmp_path / "cli-exports"
    assert cli.main(["--output-dir", str(output_dir)]) == 0
    assert (output_dir / "manifest.json").is_file()

    monkeypatch.setattr(
        cli,
        "export_public_marts",
        lambda _path: (_ for _ in ()).throw(ExportError("boom")),
    )
    assert cli.main(["--output-dir", str(tmp_path / "fail")]) == 1


def test_resolve_export_dir_defaults_and_overrides(tmp_path, monkeypatch):
    from travelcanary_pipeline.config import settings_warehouse

    monkeypatch.delenv("EXPORT_DIR", raising=False)
    reloaded = importlib.reload(settings_warehouse)
    assert reloaded.resolve_export_dir() == (reloaded.BASE_DIR / "exports").resolve()

    monkeypatch.setenv("EXPORT_DIR", "relative-exports")
    reloaded = importlib.reload(settings_warehouse)
    assert (
        reloaded.resolve_export_dir()
        == (reloaded.BASE_DIR / "relative-exports").resolve()
    )

    absolute = tmp_path / "absolute-exports"
    monkeypatch.setenv("EXPORT_DIR", str(absolute))
    reloaded = importlib.reload(settings_warehouse)
    assert reloaded.resolve_export_dir() == absolute.resolve()

    monkeypatch.delenv("EXPORT_DIR", raising=False)
    importlib.reload(settings_warehouse)
