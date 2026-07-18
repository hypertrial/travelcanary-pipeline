"""Export and re-import ``country_travel_risk_history`` across warehouse rebuilds."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from travelcanary_pipeline import __version__
from travelcanary_pipeline.public_contracts import PUBLIC_MART_COLUMNS
from travelcanary_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    ensure_duck_db,
    get_persistent_connection,
)
from travelcanary_pipeline.storage.duckdb.writer_lock import warehouse_writer_lock

HISTORY_MART = "country_travel_risk_history"
HISTORY_SCHEMA = "travelcanary_marts"
HISTORY_RELATION = f"{HISTORY_SCHEMA}.{HISTORY_MART}"
HISTORY_COLUMNS = PUBLIC_MART_COLUMNS[HISTORY_MART]
HISTORY_UNIQUE_KEY = (
    "destination_iso3",
    "issuing_government",
    "snapshot_date",
)


class HistoryTransferError(ValueError):
    """Raised when history export or import cannot proceed safely."""


def _describe_columns(conn: Any, relation: str) -> list[str]:
    return [row[0] for row in conn.execute(f"DESCRIBE {relation}").fetchall()]


def _relation_exists(conn: Any, schema: str, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, table],
    ).fetchone()
    return row is not None


def _parquet_columns(conn: Any, parquet_path: Path) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(parquet_path)],
        ).fetchall()
    ]


def _validate_history_columns(actual: list[str], *, context: str) -> None:
    expected = list(HISTORY_COLUMNS)
    if actual != expected:
        raise HistoryTransferError(
            f"column contract mismatch for {context}: "
            f"expected {expected}, found {actual}"
        )


def export_history(path: Path) -> dict[str, Any]:
    """Copy the history mart to ``path`` and write a sibling ``.manifest.json``."""
    path = Path(path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = get_persistent_connection(read_only=True)
    try:
        if not _relation_exists(conn, HISTORY_SCHEMA, HISTORY_MART):
            raise HistoryTransferError(f"history mart is missing: {HISTORY_RELATION}")
        _validate_history_columns(
            _describe_columns(conn, HISTORY_RELATION),
            context=HISTORY_RELATION,
        )
        row_count, min_date, max_date = conn.execute(
            f"""
            SELECT
                COUNT(*),
                MIN(snapshot_date),
                MAX(snapshot_date)
            FROM {HISTORY_RELATION}
            """
        ).fetchone()
        conn.execute(
            f"COPY {HISTORY_RELATION} TO ? (FORMAT PARQUET)",
            [str(path)],
        )
    finally:
        conn.close()

    manifest = {
        "package_version": __version__,
        "exported_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "relation": HISTORY_RELATION,
        "file": path.name,
        "row_count": int(row_count),
        "columns": list(HISTORY_COLUMNS),
        "snapshot_date_min": None if min_date is None else str(min_date),
        "snapshot_date_max": None if max_date is None else str(max_date),
    }
    manifest_path = path.with_name(f"{path.stem}.manifest.json")
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest["manifest_file"] = manifest_path.name
    return manifest


def import_history(path: Path) -> dict[str, Any]:
    """Import history rows whose unique key is absent; existing warehouse rows win."""
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise HistoryTransferError(f"history Parquet file not found: {path}")

    with warehouse_writer_lock(active_duckdb_path()):
        ensure_duck_db()
        conn = get_persistent_connection()
        try:
            _validate_history_columns(
                _parquet_columns(conn, path),
                context=str(path),
            )
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {HISTORY_SCHEMA}")
            if not _relation_exists(conn, HISTORY_SCHEMA, HISTORY_MART):
                conn.execute(
                    f"""
                    CREATE TABLE {HISTORY_RELATION} AS
                    SELECT * FROM read_parquet(?) LIMIT 0
                    """,
                    [str(path)],
                )
            else:
                _validate_history_columns(
                    _describe_columns(conn, HISTORY_RELATION),
                    context=HISTORY_RELATION,
                )

            column_list = ", ".join(HISTORY_COLUMNS)
            key_predicates = " AND ".join(
                f"existing.{key} = incoming.{key}" for key in HISTORY_UNIQUE_KEY
            )
            before = conn.execute(
                f"SELECT COUNT(*) FROM {HISTORY_RELATION}"
            ).fetchone()[0]
            conn.execute(
                f"""
                INSERT INTO {HISTORY_RELATION} ({column_list})
                SELECT {column_list}
                FROM read_parquet(?) AS incoming
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {HISTORY_RELATION} AS existing
                    WHERE {key_predicates}
                )
                """,
                [str(path)],
            )
            after = conn.execute(f"SELECT COUNT(*) FROM {HISTORY_RELATION}").fetchone()[
                0
            ]
            conn.commit()
        finally:
            conn.close()

    return {
        "relation": HISTORY_RELATION,
        "file": str(path),
        "rows_before": int(before),
        "rows_after": int(after),
        "rows_inserted": int(after - before),
        "policy": "existing_rows_win",
    }


__all__ = [
    "HISTORY_COLUMNS",
    "HISTORY_MART",
    "HISTORY_RELATION",
    "HISTORY_UNIQUE_KEY",
    "HistoryTransferError",
    "export_history",
    "import_history",
]
