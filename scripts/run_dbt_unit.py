"""Build and test the isolated dbt unit-test warehouse under one writer lock."""

from __future__ import annotations

import subprocess

from seed_ci_warehouse import _reset_and_seed

from travelcanary_pipeline.config.settings import DBT_PROFILES_DIR, DBT_PROJECT_DIR
from travelcanary_pipeline.config.settings_warehouse import dbt_cli_argv
from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    reset_duckdb_connection_state,
)
from travelcanary_pipeline.storage.duckdb.writer_lock import warehouse_writer_lock


def main() -> int:
    contracts = load_source_contracts()
    reset_duckdb_connection_state()
    warehouse = active_duckdb_path()
    with warehouse_writer_lock(warehouse):
        _reset_and_seed(warehouse, contracts)
        commands = (
            ("seed",),
            ("run", "--empty"),
            ("test", "--select", "test_type:unit"),
        )
        for command in commands:
            result = subprocess.run(
                dbt_cli_argv(
                    *command,
                    "--project-dir",
                    str(DBT_PROJECT_DIR),
                    "--profiles-dir",
                    str(DBT_PROFILES_DIR),
                ),
                check=False,
            )
            if result.returncode:
                return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
