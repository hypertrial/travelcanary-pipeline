import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = (REPO_ROOT / "Makefile").read_text()


def test_makefile_exposes_ci_parity_targets():
    for target in (
        "coverage-erase",
        "test-cov",
        "integration-dagster-cov",
        "integration-dbt-cov",
        "coverage-report",
        "dbt-unit",
        "golden-dbt",
        "contract-http",
    ):
        assert f"{target}:" in MAKEFILE


def test_coverage_targets_append_before_the_final_report():
    assert (
        "COV_APPEND_ARGS := --cov=travelcanary_pipeline --cov-branch --cov-append"
        in MAKEFILE
    )
    assert "coverage report --show-missing --fail-under=100" in MAKEFILE
    assert "not contract" not in MAKEFILE
    assert "--ignore=tests/integration --ignore=tests/dbt" in MAKEFILE


def test_contract_and_golden_targets_select_their_canonical_suites():
    assert "tests/contract" in _make_dry_run("contract-http")
    assert "tests/integration/duckdb/test_golden_marts.py" in _make_dry_run(
        "golden-dbt"
    )
    assert "tests/integration/duckdb tests/dbt" in _make_dry_run("integration-dbt-cov")


def _make_dry_run(target: str) -> str:
    return subprocess.run(
        ["make", "-n", target],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def test_demo_reuses_seeded_build_at_disposable_path():
    output = _make_dry_run("demo")
    assert ".cache/travelcanary_demo.duckdb" in output
    assert ".cache/dbt_build.duckdb" not in output
    for command in (
        "scripts/seed_ci_warehouse.py",
        "scripts/run_atomic_dbt_build.py",
        "scripts/validate_ci_warehouse.py",
    ):
        assert command in output


def test_demo_ui_opens_the_disposable_demo_warehouse():
    output = _make_dry_run("demo-ui")
    assert 'duckdb "' in output
    assert ".cache/travelcanary_demo.duckdb" in output
    assert " -ui" in output


def test_dbt_build_and_test_use_the_atomic_wrapper_commands():
    assert "scripts/run_atomic_dbt_build.py build" in _make_dry_run("dbt-build")
    assert "scripts/run_atomic_dbt_build.py test" in _make_dry_run("dbt-test")


def test_dbt_unit_uses_the_locked_isolated_wrapper():
    output = _make_dry_run("dbt-unit")
    assert "scripts/run_dbt_unit.py" in output
    assert "dbt.cli.main" not in output


def _run_costguard_preflight(tmp_path, version: str | None):
    path = "/usr/bin:/bin"
    if version is not None:
        binary = tmp_path / "costguard"
        binary.write_text(f"#!/bin/sh\nprintf 'costguard {version}\\n'\n")
        binary.chmod(0o755)
        path = f"{tmp_path}:{path}"
    return subprocess.run(
        ["make", "check-costguard-version"],
        cwd=REPO_ROOT,
        env={**os.environ, "PATH": path},
        capture_output=True,
        text=True,
    )


def test_costguard_preflight_reports_missing_binary_and_pinned_install(tmp_path):
    result = _run_costguard_preflight(tmp_path, None)

    assert result.returncode != 0
    assert "Costguard 2.5.0 is required" in result.stderr
    assert "install.sh | sh -s -- v2.5.0" in result.stderr


def test_costguard_preflight_reports_outdated_binary(tmp_path):
    result = _run_costguard_preflight(tmp_path, "2.4.0")

    assert result.returncode != 0
    assert "found costguard 2.4.0" in result.stderr
    assert "install.sh | sh -s -- v2.5.0" in result.stderr


def test_costguard_preflight_accepts_exact_pinned_version(tmp_path):
    result = _run_costguard_preflight(tmp_path, "2.5.0")

    assert result.returncode == 0, result.stderr
