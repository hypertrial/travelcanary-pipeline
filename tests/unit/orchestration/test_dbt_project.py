from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from travelcanary_pipeline.config.settings import (
    BASE_DIR,
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    dbt_cli_argv,
)

DBT_TARGET_PATH = Path("target")


@pytest.fixture
def manifest_path() -> Path:
    return DBT_PROJECT_DIR / "target" / "manifest.json"


def test_ensure_dbt_manifest_skips_parse_when_manifest_exists(manifest_path: Path):
    with (
        patch(
            "travelcanary_pipeline.orchestration.dbt_project.DBT_MANIFEST_PATH",
            manifest_path,
        ),
        patch(
            "travelcanary_pipeline.orchestration.dbt_project.DBT_PROJECT"
        ) as mock_project,
        patch(
            "travelcanary_pipeline.orchestration.dbt_project.subprocess.run"
        ) as mock_run,
        patch.object(Path, "exists", return_value=True),
    ):
        from travelcanary_pipeline.orchestration.dbt_project import ensure_dbt_manifest

        result = ensure_dbt_manifest()

    mock_project.prepare_if_dev.assert_called_once_with()
    mock_run.assert_not_called()
    assert result == manifest_path


def test_ensure_dbt_manifest_runs_parse_when_manifest_missing(manifest_path: Path):
    with (
        patch(
            "travelcanary_pipeline.orchestration.dbt_project.DBT_MANIFEST_PATH",
            manifest_path,
        ),
        patch(
            "travelcanary_pipeline.orchestration.dbt_project.DBT_PROJECT"
        ) as mock_project,
        patch(
            "travelcanary_pipeline.orchestration.dbt_project.subprocess.run"
        ) as mock_run,
        patch.object(Path, "exists", return_value=False),
    ):
        from travelcanary_pipeline.orchestration.dbt_project import ensure_dbt_manifest

        result = ensure_dbt_manifest()

    mock_project.prepare_if_dev.assert_called_once_with()
    mock_run.assert_called_once_with(
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
    assert result == manifest_path
