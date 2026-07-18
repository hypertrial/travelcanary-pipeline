"""Contract-validated Parquet export of public TravelCanary marts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from travelcanary_pipeline import __version__
from travelcanary_pipeline.public_contracts import PUBLIC_MART_COLUMNS, PUBLIC_MARTS
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection
from travelcanary_pipeline.storage.duckdb.relations import (
    describe_columns,
    relation_exists,
)

MARTS_SCHEMA = "travelcanary_marts"


class ExportError(ValueError):
    """Raised when a public mart cannot be exported as contracted."""


def _validate_mart_columns(conn: Any, mart: str) -> str:
    relation = f"{MARTS_SCHEMA}.{mart}"
    if not relation_exists(conn, MARTS_SCHEMA, mart):
        raise ExportError(f"public mart is missing: {relation}")
    actual = describe_columns(conn, relation)
    expected = PUBLIC_MART_COLUMNS[mart]
    if actual != expected:
        raise ExportError(
            f"column contract mismatch for {relation}: "
            f"expected {expected}, found {actual}"
        )
    return relation


def export_public_marts(output_dir: Path) -> dict[str, Any]:
    """Export every public mart to Parquet under ``output_dir`` with a manifest."""
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_persistent_connection(read_only=True)
    marts: dict[str, Any] = {}
    try:
        for mart in PUBLIC_MARTS:
            relation = _validate_mart_columns(conn, mart)
            row_count = conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
            parquet_path = output_dir / f"{mart}.parquet"
            conn.execute(
                f"COPY {relation} TO ? (FORMAT PARQUET)",
                [str(parquet_path)],
            )
            marts[mart] = {
                "file": parquet_path.name,
                "row_count": int(row_count),
                "columns": list(PUBLIC_MART_COLUMNS[mart]),
            }
    finally:
        conn.close()

    manifest = {
        "package_version": __version__,
        "exported_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "marts": marts,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


__all__ = ["ExportError", "export_public_marts"]
