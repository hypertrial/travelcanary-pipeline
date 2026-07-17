from __future__ import annotations

import os
import subprocess
from pathlib import Path

from dagster_dbt import DbtProject

from travelcanary_pipeline.config.settings import (
    BASE_DIR,
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    dbt_cli_argv,
)

DBT_TARGET_PATH = Path(os.getenv("DBT_TARGET_PATH", "target"))
DBT_PROJECT = DbtProject(project_dir=DBT_PROJECT_DIR, target_path=DBT_TARGET_PATH)
DBT_MANIFEST_PATH = DBT_PROJECT.manifest_path


def ensure_dbt_manifest() -> Path:
    """Guarantee a loadable dbt manifest exists before dbt_assets() is defined.

    DbtProject.prepare_if_dev() only regenerates the manifest inside `dagster dev`
    (DAGSTER_IS_DEV_CLI); outside that context (CI, fresh checkout, direct pytest
    collection) it is a no-op. Fall back to the same `dbt parse` invocation the
    Makefile's dbt-parse target uses, so importing this module never depends on
    external step ordering or a stale manifest left over from a previous run.
    """
    DBT_PROJECT.prepare_if_dev()
    if not DBT_MANIFEST_PATH.exists():
        subprocess.run(
            [
                *dbt_cli_argv("parse", "--quiet"),
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
                "--target-path",
                str(DBT_TARGET_PATH),
            ],
            check=True,
            cwd=BASE_DIR,
        )
    return DBT_MANIFEST_PATH


ensure_dbt_manifest()

__all__ = ["DBT_MANIFEST_PATH", "DBT_PROJECT", "DBT_TARGET_PATH", "ensure_dbt_manifest"]
