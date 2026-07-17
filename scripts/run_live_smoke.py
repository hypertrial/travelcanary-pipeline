"""Run opt-in live readiness through audit, Dagster, dbt, and warehouse checks."""

from __future__ import annotations

import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LIVE_DUCKDB_PATH = REPO_ROOT / ".cache" / "live_smoke.duckdb"
LIVE_DBT_TARGET_PATH = REPO_ROOT / ".cache" / "live_smoke_dbt_target"
LIVE_DLT_DATA_DIR = REPO_ROOT / ".cache" / "live_smoke_dlt"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _configure_disposable_warehouse() -> None:
    LIVE_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    os.environ["DUCKDB_PATH"] = str(LIVE_DUCKDB_PATH)
    os.environ["DUCKDB_NAME"] = str(LIVE_DUCKDB_PATH)
    os.environ["DBT_PROFILES_DIR"] = str(REPO_ROOT / "dbt" / "profiles")
    os.environ["DBT_TARGET_PATH"] = str(LIVE_DBT_TARGET_PATH)
    os.environ["DBT_LOG_PATH"] = str(LIVE_DBT_TARGET_PATH)
    os.environ["DLT_DATA_DIR"] = str(LIVE_DLT_DATA_DIR)


def _reset_disposable_warehouse() -> None:
    if LIVE_DUCKDB_PATH.parent != REPO_ROOT / ".cache":
        raise RuntimeError(f"refusing to reset unexpected path: {LIVE_DUCKDB_PATH}")
    for path in (
        LIVE_DUCKDB_PATH,
        LIVE_DUCKDB_PATH.with_suffix(".duckdb.wal"),
        LIVE_DUCKDB_PATH.with_name(f"{LIVE_DUCKDB_PATH.name}-wal"),
        LIVE_DUCKDB_PATH.with_name(f"{LIVE_DUCKDB_PATH.name}-shm"),
    ):
        path.unlink(missing_ok=True)
    for path in (LIVE_DBT_TARGET_PATH, LIVE_DLT_DATA_DIR):
        if path.parent != REPO_ROOT / ".cache":
            raise RuntimeError(f"refusing to reset unexpected path: {path}")
        shutil.rmtree(path, ignore_errors=True)


def _materialize_full_pipeline() -> None:
    from dagster import materialize
    from dagster_dbt import DbtCliResource

    from travelcanary_pipeline.config.settings import DBT_PROFILES_DIR, DBT_PROJECT_DIR
    from travelcanary_pipeline.orchestration.assets import (
        ca_gac_raw_advisories,
        gdelt_raw_events,
        jp_mofa_raw_advisories,
        nl_mfa_raw_advisories,
        travelcanary_dbt,
        uk_fcdo_raw_advisories,
        us_state_raw_advisories,
    )
    from travelcanary_pipeline.storage.duckdb.connection import (
        reset_duckdb_connection_state,
    )

    reset_duckdb_connection_state()
    result = materialize(
        [
            us_state_raw_advisories,
            ca_gac_raw_advisories,
            uk_fcdo_raw_advisories,
            nl_mfa_raw_advisories,
            jp_mofa_raw_advisories,
            gdelt_raw_events,
            travelcanary_dbt,
        ],
        resources={
            "dbt": DbtCliResource(
                project_dir=str(DBT_PROJECT_DIR),
                profiles_dir=str(DBT_PROFILES_DIR),
            )
        },
    )
    if not result.success:
        raise RuntimeError("Dagster materialization failed")


def _print_required_blockers(results: list[dict[str, object]]) -> None:
    print(
        "Live smoke stopped before Dagster materialization because a required "
        "source audit failed.",
        file=sys.stderr,
    )
    for row in results:
        if row["role"] == "required" and row["status"] != "accepted":
            print(
                f"{row['source']}: {row['status']} - {row.get('reason')}",
                file=sys.stderr,
            )


def main() -> int:
    _configure_disposable_warehouse()
    _reset_disposable_warehouse()

    from travelcanary_pipeline.live_audit import (
        audit_sources,
        has_blocking_required_failure,
    )

    target_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    print("Running full read-only source audit before live smoke.", file=sys.stderr)
    results = audit_sources(target_date)
    print(json.dumps(results, indent=2, sort_keys=True))
    if has_blocking_required_failure(results):
        _print_required_blockers(results)
        return 1

    print(
        f"Materializing Dagster full pipeline into {LIVE_DUCKDB_PATH}.", file=sys.stderr
    )
    _materialize_full_pipeline()

    from scripts.validate_live_warehouse import validate_live_warehouse

    print(
        f"Validating disposable live-smoke warehouse at {LIVE_DUCKDB_PATH}.",
        file=sys.stderr,
    )
    print(f"Live smoke validated: {validate_live_warehouse()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
