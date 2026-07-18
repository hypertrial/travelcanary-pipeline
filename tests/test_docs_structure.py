from __future__ import annotations

import re
from pathlib import Path

import yaml

from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.public_contracts import (
    PUBLIC_DAGSTER_JOBS,
    PUBLIC_MART_COLUMNS,
    PUBLIC_MARTS,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = REPO_ROOT / "docs"

EXPECTED_NAV = [
    "Overview",
    "Quickstart",
    "Consumer guide",
    "Architecture and lineage",
    "Operations and recovery",
    "Source coverage and caveats",
    "Warehouse",
    "Data contracts",
    "Configuration",
    "Naming",
    "Development",
    "Troubleshooting",
    "Legal and privacy",
    "Changelog",
]


def _all_docs() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOCS.glob("*.md"))


def test_mkdocs_navigation_is_complete_and_files_exist():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8"))
    assert config["strict"] is True
    assert config["theme"]["name"] == "material"
    assert [next(iter(item)) for item in config["nav"]] == EXPECTED_NAV
    for item in config["nav"]:
        assert (DOCS / next(iter(item.values()))).is_file()


def test_documented_public_names_match_code():
    documentation = _all_docs()
    mart_files = {path.stem for path in (REPO_ROOT / "dbt/models/marts").glob("*.sql")}
    assert mart_files == set(PUBLIC_MARTS)
    documented_marts = {name for name in PUBLIC_MARTS if name in documentation}
    assert set(PUBLIC_MARTS) <= documented_marts

    jobs_code = (
        REPO_ROOT / "src/travelcanary_pipeline/orchestration/jobs.py"
    ).read_text(encoding="utf-8")
    for job in PUBLIC_DAGSTER_JOBS:
        assert f'name="{job}"' in jobs_code
        assert job in documentation


def test_public_mart_contract_is_fully_documented():
    catalog = yaml.safe_load(
        (REPO_ROOT / "dbt/models/marts/marts.yml").read_text(encoding="utf-8")
    )
    models = {model["name"]: model for model in catalog["models"]}
    assert set(models) == set(PUBLIC_MART_COLUMNS)
    for model_name, expected_columns in PUBLIC_MART_COLUMNS.items():
        model = models[model_name]
        assert model["description"].strip()
        documented_columns = model["columns"]
        assert [column["name"] for column in documented_columns] == expected_columns
        for column in documented_columns:
            assert column["description"].strip(), f"{model_name}.{column['name']}"


def test_environment_inventory_matches_documentation():
    env_text = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    env_names = set(re.findall(r"^([A-Z][A-Z0-9_]+)=", env_text, re.MULTILINE))
    config_doc = (DOCS / "configuration.md").read_text(encoding="utf-8")
    assert env_names
    for name in env_names:
        assert f"`{name}`" in config_doc
    assert "TRAVEL_ADVISORY_INFO_VERIFY_SSL" not in env_names


def test_live_operator_commands_are_documented_and_implemented():
    documentation = _all_docs()
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    for command in ("source-audit", "live-smoke"):
        assert f"{command}:" in makefile
        assert command in documentation
    for cli_flag in ("--source", "--output", "--propose-floors", "--warehouse"):
        assert cli_flag in documentation
    assert (
        "GitHub Actions never runs live-source audits or live ingestion"
        in documentation
    )


def test_offline_demo_commands_are_documented_and_implemented():
    documentation = _all_docs()
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    for command in ("demo", "demo-ui"):
        assert f"{command}:" in makefile
        assert f"make {command}" in documentation
    assert ".cache/travelcanary_demo.duckdb" in documentation


def test_offline_contract_commands_are_documented_and_implemented():
    documentation = _all_docs()
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    for command in ("contract-http", "golden-dbt"):
        assert f"{command}:" in makefile
        assert f"make {command}" in documentation

    fixtures = REPO_ROOT / "tests/fixtures/contracts"
    assert len(list(fixtures.iterdir())) == len(load_source_contracts())
    assert all(path.stat().st_size < 10_000 for path in fixtures.iterdir())


def test_export_and_history_commands_are_documented_and_implemented():
    documentation = _all_docs()
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    for command in ("export-marts", "export-history", "import-history"):
        assert f"{command}:" in makefile
        assert command in documentation
    assert "existing warehouse rows win" in documentation.lower()
    assert "EXPORT_DIR" in documentation
    assert "make export-marts" in documentation
    assert "make export-history" in documentation
    assert "make import-history" in documentation


def test_strict_configuration_and_costguard_prerequisite_are_documented():
    documentation = _all_docs()

    assert "positive integers" in documentation
    assert "0`, `false`, `no`, or `off" in documentation
    assert "profiles.yml" in documentation
    assert "costguard/main/scripts/install.sh | sh -s -- v2.5.0" in documentation
    assert "detected binary" in documentation
    assert "automatically" in documentation
    assert "Blank `DUCKDB_PATH` and `DUCKDB_NAME`" in documentation
    assert "must be a file" in documentation


def test_atomic_publication_and_parse_diagnostics_are_documented():
    documentation = _all_docs()
    for term in (
        "writer.lock",
        "same-directory candidate",
        "atomically",
        "discovered_rows",
        "blocking_skipped_rows",
        "travelcanary_pipeline.live_audit",
    ):
        assert term in documentation


def test_ci_workflow_is_one_bounded_offline_runner():
    workflow_path = REPO_ROOT / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    workflow_text = workflow_path.read_text(encoding="utf-8")

    assert set(workflow["jobs"]) == {"fast-gate"}
    assert workflow["jobs"]["fast-gate"]["timeout-minutes"] == 5
    assert "uv run make lint test dbt-parse docs-build" in workflow_text
    assert "live-smoke" not in workflow_text
    assert "source-audit" not in workflow_text
    assert not (workflow_path.parent / "live-readiness.yml").exists()
    assert sorted(path.name for path in workflow_path.parent.glob("*.yml")) == [
        "ci.yml",
        "docs.yml",
    ]


def test_docs_workflow_deploys_only_on_version_tags():
    workflow_path = REPO_ROOT / ".github/workflows/docs.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    workflow_text = workflow_path.read_text(encoding="utf-8")
    trigger = workflow.get("on", workflow.get(True))
    assert trigger["push"]["tags"] == ["v*"]
    assert "pull_request" not in trigger
    assert workflow["permissions"]["contents"] == "write"
    assert "mkdocs gh-deploy" in workflow_text
    assert "timeout-minutes" in workflow_text
