"""Shared DuckDB relation helpers for public mart I/O."""

from __future__ import annotations

from typing import Any


def describe_columns(conn: Any, relation: str) -> list[str]:
    return [row[0] for row in conn.execute(f"DESCRIBE {relation}").fetchall()]


def relation_exists(conn: Any, schema: str, table: str) -> bool:
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


__all__ = ["describe_columns", "relation_exists"]
