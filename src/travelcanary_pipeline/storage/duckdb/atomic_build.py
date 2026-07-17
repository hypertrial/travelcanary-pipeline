"""Build dbt against a candidate DuckDB file and atomically publish it."""

from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import duckdb

from travelcanary_pipeline.storage.duckdb.connection import (
    reset_duckdb_connection_state,
)
from travelcanary_pipeline.storage.duckdb.writer_lock import warehouse_writer_lock


def _artifacts(path: Path) -> tuple[Path, ...]:
    return (
        path,
        Path(f"{path}.wal"),
        Path(f"{path}-wal"),
        Path(f"{path}-shm"),
    )


def _remove_artifacts(path: Path) -> None:
    for artifact in _artifacts(path):
        artifact.unlink(missing_ok=True)


def _checkpoint(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def _candidate_prefix(primary: Path) -> str:
    warehouse_id = sha256(primary.name.encode()).hexdigest()[:12]
    return f"travelcanary_dbt_candidate_{warehouse_id}_"


def _clean_stale_candidates(primary: Path) -> None:
    for path in primary.parent.glob(f"{_candidate_prefix(primary)}*"):
        if path.is_file():
            path.unlink()


@contextmanager
def atomic_dbt_warehouse(primary_path: Path) -> Iterator[Path]:
    primary = primary_path.expanduser().resolve()
    candidate = primary.with_name(
        f"{_candidate_prefix(primary)}{uuid4().hex}{primary.suffix or '.duckdb'}"
    )
    old_path = os.environ.get("DUCKDB_PATH")
    with warehouse_writer_lock(primary):
        primary.parent.mkdir(parents=True, exist_ok=True)
        _checkpoint(primary)
        _clean_stale_candidates(primary)
        try:
            shutil.copy2(primary, candidate)
            os.environ["DUCKDB_PATH"] = str(candidate)
            reset_duckdb_connection_state()
            yield candidate
            reset_duckdb_connection_state()
            _checkpoint(candidate)
            os.replace(candidate, primary)
        finally:
            _remove_artifacts(candidate)
            if old_path is None:
                os.environ.pop("DUCKDB_PATH", None)
            else:
                os.environ["DUCKDB_PATH"] = old_path
            reset_duckdb_connection_state()


__all__ = ["atomic_dbt_warehouse"]
