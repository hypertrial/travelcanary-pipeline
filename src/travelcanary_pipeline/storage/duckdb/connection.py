"""DuckDB connection management."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import duckdb

from travelcanary_pipeline.config import settings as _settings
from travelcanary_pipeline.naming import ALL_ADVISORY_SOURCES, raw_schema

logger = logging.getLogger(__name__)

_SCHEMA_INITIALIZED = False
_ACTIVE_DUCKDB_PATH: Path | None = None


def _resolved_duckdb_path() -> Path:
    return _settings.resolve_duckdb_path()


def _reset_dlt_pipelines() -> None:
    from travelcanary_pipeline.ingestion.common.dlt_runner import reset_dlt_pipelines

    reset_dlt_pipelines()


def reset_duckdb_connection_state() -> None:
    global _SCHEMA_INITIALIZED, _ACTIVE_DUCKDB_PATH
    _SCHEMA_INITIALIZED = False
    _ACTIVE_DUCKDB_PATH = None
    _reset_dlt_pipelines()


def _sync_active_duckdb_path() -> Path:
    global _SCHEMA_INITIALIZED, _ACTIVE_DUCKDB_PATH
    path = _resolved_duckdb_path()
    if _ACTIVE_DUCKDB_PATH != path:
        should_reset = _ACTIVE_DUCKDB_PATH is not None or _SCHEMA_INITIALIZED
        _ACTIVE_DUCKDB_PATH = path
        if should_reset:
            _SCHEMA_INITIALIZED = False
            _reset_dlt_pipelines()
    return path


def active_duckdb_path() -> Path:
    return _ACTIVE_DUCKDB_PATH or _sync_active_duckdb_path()


def is_duckdb_lock_io_error(exc: BaseException) -> bool:
    if not isinstance(exc, duckdb.IOException):
        return False
    msg = str(exc).lower()
    return "conflicting lock" in msg or "could not set lock" in msg


def _configure_duckdb_session(
    conn: duckdb.DuckDBPyConnection, *, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    memory_limit = _settings.resolve_duckdb_memory_limit()
    if memory_limit:
        conn.execute("SET memory_limit = ?", [memory_limit])
    if not read_only:
        conn.execute("SET preserve_insertion_order = false")
    return conn


def _connect_duckdb(
    path: Optional[Path] = None, *, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    global _ACTIVE_DUCKDB_PATH
    if path is None:
        path = _sync_active_duckdb_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        return _configure_duckdb_session(
            duckdb.connect(str(path), read_only=read_only),
            read_only=read_only,
        )
    except duckdb.IOException as exc:
        if os.getenv("PYTEST_CURRENT_TEST") and is_duckdb_lock_io_error(exc):
            worker = os.getenv("PYTEST_XDIST_WORKER", "gw0")
            tmp_dir = Path(tempfile.gettempdir())
            alt_path = (
                tmp_dir / f"{path.stem}.pytest.{worker}.{os.getpid()}{path.suffix}"
            )
            logger.warning("DuckDB locked at %s; using %s", path, alt_path)
            _ACTIVE_DUCKDB_PATH = alt_path
            return _configure_duckdb_session(
                duckdb.connect(str(_ACTIVE_DUCKDB_PATH)),
                read_only=False,
            )
        raise


def bootstrap_raw_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    for source in ALL_ADVISORY_SOURCES:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {raw_schema(source)}")
    from travelcanary_pipeline.ingestion.gdelt.events import ensure_gdelt_events_table
    from travelcanary_pipeline.storage.duckdb.source_runs import (
        ensure_source_runs_table,
    )

    ensure_gdelt_events_table(conn)
    ensure_source_runs_table(conn)


def ensure_duck_db() -> None:
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    conn = _connect_duckdb()
    try:
        bootstrap_raw_schemas(conn)
    finally:
        conn.close()
    _SCHEMA_INITIALIZED = True


def get_persistent_connection(*, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    ensure_duck_db()
    return _connect_duckdb(read_only=read_only)


__all__ = [
    "active_duckdb_path",
    "bootstrap_raw_schemas",
    "ensure_duck_db",
    "get_persistent_connection",
    "is_duckdb_lock_io_error",
    "reset_duckdb_connection_state",
]
