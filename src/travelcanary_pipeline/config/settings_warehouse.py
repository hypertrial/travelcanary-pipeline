"""Warehouse paths, dotenv bootstrap, DuckDB/dbt dirs."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

from travelcanary_pipeline.config._env import _optional_env_str

PACKAGE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PACKAGE_DIR.parent
BASE_DIR = SRC_DIR.parent

env_path = BASE_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

_DEFAULT_DUCKDB_NAME = "travelcanary.duckdb"


def resolve_duckdb_path() -> Path:
    path_value = _optional_env_str("DUCKDB_PATH")
    name_value = _optional_env_str("DUCKDB_NAME") or _DEFAULT_DUCKDB_NAME
    if path_value:
        path = Path(path_value).expanduser().resolve()
    else:
        name_path = Path(name_value).expanduser()
        path = (
            name_path if name_path.is_absolute() else BASE_DIR / name_path
        ).resolve()
    if path.exists() and path.is_dir():
        raise ValueError(
            f"DuckDB warehouse path must be a file, not a directory: {path}"
        )
    return path


DUCKDB_NAME = _optional_env_str("DUCKDB_NAME") or _DEFAULT_DUCKDB_NAME
DUCKDB_PATH = resolve_duckdb_path()

DBT_PROJECT_DIR = BASE_DIR / "dbt"
_DEFAULT_DBT_PROFILES_DIR = BASE_DIR / "dbt" / "profiles"
_ENV_DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
DBT_PROFILES_DIR = (
    Path(_ENV_DBT_PROFILES_DIR).expanduser()
    if _ENV_DBT_PROFILES_DIR and _ENV_DBT_PROFILES_DIR.strip()
    else _DEFAULT_DBT_PROFILES_DIR
)
_profiles_yml = DBT_PROFILES_DIR / "profiles.yml"


def _has_valid_travelcanary_profile(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        profiles = yaml.safe_load(path.read_text())
    except yaml.YAMLError:
        return False
    if not isinstance(profiles, dict):
        return False
    profile = profiles.get("travelcanary")
    if not isinstance(profile, dict):
        return False
    target = profile.get("target")
    outputs = profile.get("outputs")
    if not isinstance(target, str) or not target.strip():
        return False
    if not isinstance(outputs, dict):
        return False
    output = outputs.get(target)
    if not isinstance(output, dict) or output.get("type") != "duckdb":
        return False
    path = output.get("path")
    return isinstance(path, str) and bool(path.strip())


if (
    _ENV_DBT_PROFILES_DIR
    and _ENV_DBT_PROFILES_DIR.strip()
    and not _has_valid_travelcanary_profile(_profiles_yml)
):
    raise ValueError(
        "DBT_PROFILES_DIR must contain profiles.yml with a valid travelcanary profile"
    )
os.environ["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)


def dbt_cli_argv(*args: str) -> list[str]:
    return [sys.executable, "-m", "dbt.cli.main", *args]


def resolve_dbt_executable() -> str:
    venv_dbt = Path(sys.executable).with_name("dbt")
    if venv_dbt.is_file():
        return str(venv_dbt)
    return shutil.which("dbt") or "dbt"


__all__ = [
    "BASE_DIR",
    "DBT_PROFILES_DIR",
    "DBT_PROJECT_DIR",
    "DUCKDB_NAME",
    "DUCKDB_PATH",
    "PACKAGE_DIR",
    "SRC_DIR",
    "dbt_cli_argv",
    "resolve_dbt_executable",
    "resolve_duckdb_path",
]
