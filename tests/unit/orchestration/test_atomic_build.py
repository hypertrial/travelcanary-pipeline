from __future__ import annotations

import os
import subprocess
import sys

import duckdb
import pytest

from travelcanary_pipeline.storage.duckdb.atomic_build import (
    _candidate_prefix,
    atomic_dbt_warehouse,
)
from travelcanary_pipeline.storage.duckdb.writer_lock import (
    warehouse_writer_lock,
    writer_lock_path,
)


def _value(path) -> str:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return str(conn.execute("select value from state").fetchone()[0])
    finally:
        conn.close()


def _warehouse(path, value: str = "old") -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("create table state(value varchar)")
        conn.execute("insert into state values (?)", [value])
    finally:
        conn.close()


def test_atomic_build_promotes_candidate_and_restores_environment(
    tmp_path, monkeypatch
):
    primary = tmp_path / "warehouse.duckdb"
    _warehouse(primary)
    monkeypatch.setenv("DUCKDB_PATH", str(primary))
    stale = primary.with_name(f"{_candidate_prefix(primary)}stale.duckdb")
    stale.write_text("stale")
    unrelated_directory = primary.with_name(f"{_candidate_prefix(primary)}directory")
    unrelated_directory.mkdir()

    with atomic_dbt_warehouse(primary) as candidate:
        assert os.environ["DUCKDB_PATH"] == str(candidate)
        conn = duckdb.connect(str(candidate))
        conn.execute("update state set value = 'new'")
        conn.close()

    assert _value(primary) == "new"
    assert os.environ["DUCKDB_PATH"] == str(primary)
    assert not stale.exists()
    assert unrelated_directory.is_dir()
    unrelated_directory.rmdir()
    assert not list(tmp_path.glob(f"{_candidate_prefix(primary)}*"))


def test_atomic_build_failure_preserves_primary_and_unset_environment(
    tmp_path, monkeypatch
):
    primary = tmp_path / "warehouse.duckdb"
    _warehouse(primary)
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    with pytest.raises(RuntimeError, match="dbt failed"):
        with atomic_dbt_warehouse(primary) as candidate:
            conn = duckdb.connect(str(candidate))
            conn.execute("update state set value = 'partial'")
            conn.close()
            raise RuntimeError("dbt failed")

    assert _value(primary) == "old"
    assert "DUCKDB_PATH" not in os.environ
    assert not list(tmp_path.glob(f"{_candidate_prefix(primary)}*"))


def test_atomic_build_cleans_partial_candidate_when_copy_fails(tmp_path, monkeypatch):
    primary = tmp_path / "warehouse.duckdb"
    _warehouse(primary)

    def fail_copy(_source, destination):
        destination.write_text("partial")
        raise OSError("copy failed")

    monkeypatch.setattr(
        "travelcanary_pipeline.storage.duckdb.atomic_build.shutil.copy2",
        fail_copy,
    )

    with pytest.raises(OSError, match="copy failed"):
        with atomic_dbt_warehouse(primary):
            pass

    assert _value(primary) == "old"
    assert not list(tmp_path.glob(f"{_candidate_prefix(primary)}*"))


def test_stale_cleanup_is_scoped_to_the_exact_warehouse_name(tmp_path):
    primary = tmp_path / "warehouse.duckdb"
    same_stem = tmp_path / "warehouse.db"
    _warehouse(primary)
    other_candidate = same_stem.with_name(
        f"{_candidate_prefix(same_stem)}interrupted.db"
    )
    other_candidate.write_text("other warehouse")

    with atomic_dbt_warehouse(primary):
        pass

    assert other_candidate.read_text() == "other warehouse"


def test_writer_lock_rejects_contention_without_blocking_readers(tmp_path):
    primary = tmp_path / "warehouse.duckdb"
    _warehouse(primary)

    with warehouse_writer_lock(primary):
        assert _value(primary) == "old"
        with pytest.raises(RuntimeError, match="writer already active"):
            with warehouse_writer_lock(primary):
                pass

    assert writer_lock_path(primary).is_file()


def test_writer_lock_is_released_when_process_exits(tmp_path):
    primary = tmp_path / "warehouse.duckdb"
    code = """
import os
import sys
from pathlib import Path
from travelcanary_pipeline.storage.duckdb.writer_lock import warehouse_writer_lock
with warehouse_writer_lock(Path(sys.argv[1])):
    os._exit(0)
"""
    subprocess.run([sys.executable, "-c", code, str(primary)], check=True)

    with warehouse_writer_lock(primary):
        pass
