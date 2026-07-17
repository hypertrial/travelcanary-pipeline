"""Run a dbt mutation against a candidate warehouse and publish on success."""

from __future__ import annotations

import subprocess
import sys

from travelcanary_pipeline.config.settings import DBT_PROFILES_DIR, DBT_PROJECT_DIR
from travelcanary_pipeline.config.settings_warehouse import dbt_cli_argv
from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.storage.duckdb.atomic_build import atomic_dbt_warehouse
from travelcanary_pipeline.storage.duckdb.connection import active_duckdb_path


def main(command: str = "build") -> int:
    if command not in {"build", "test"}:
        raise ValueError(f"unsupported dbt command: {command}")
    load_source_contracts()
    with atomic_dbt_warehouse(active_duckdb_path()):
        result = subprocess.run(
            dbt_cli_argv(
                command,
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
            ),
            check=False,
        )
        if result.returncode:
            raise SystemExit(result.returncode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1] if len(sys.argv) > 1 else "build"))
