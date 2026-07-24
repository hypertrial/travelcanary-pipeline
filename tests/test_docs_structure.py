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

EXPECTED_TOP_NAV = [
    "Home",
    "Audiences",
    "Get started",
    "Guides",
    "Reference",
    "Concepts",
    "Development",
]


def _nav_targets(items):
    for item in items:
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            for value in item.values():
                if isinstance(value, str):
                    yield value
                else:
                    yield from _nav_targets(value)


def _all_docs() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(DOCS.rglob("*.md"))
    )


def _config():
    text = (REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    text = re.sub(r"!!python/name:([^\s]+)", r"\1", text)
    return yaml.safe_load(text)


def test_mkdocs_navigation_contains_every_docs_page():
    config = _config()
    assert config["strict"] is True
    assert config["theme"]["name"] == "material"
    assert [next(iter(item)) for item in config["nav"]] == EXPECTED_TOP_NAV
    targets = set(_nav_targets(config["nav"]))
    pages = {path.relative_to(DOCS).as_posix() for path in DOCS.rglob("*.md")}
    assert targets == pages
    for target in targets:
        assert (DOCS / target).is_file(), target


def test_every_page_starts_with_a_visible_h1():
    for path in DOCS.rglob("*.md"):
        text = path.read_text(encoding="utf-8")
        assert re.search(r"^# [^#]", text, re.MULTILINE), path.relative_to(DOCS)


def test_mkdocs_theme_floor():
    config = _config()
    assert config["site_url"] == "https://hypertrial.github.io/travelcanary-pipeline/"
    assert config["theme"]["font"] is False
    features = set(config["theme"]["features"])
    for required in (
        "navigation.tabs",
        "navigation.sections",
        "navigation.indexes",
        "content.code.copy",
        "search.suggest",
    ):
        assert required in features
    assert "search" in config["plugins"]
    assert "assets/stylesheets/extra.css" in config["extra_css"]


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
    config_doc = (DOCS / "reference/configuration.md").read_text(encoding="utf-8")
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
    assert "required before tagging a release" in documentation.lower()
    agents = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    contributing = (REPO_ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    for text in (agents, contributing, documentation):
        assert "make source-audit" in text or "source-audit" in text
        assert "make live-smoke" in text or "live-smoke" in text
        assert "before tagging" in text.lower()


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
    assert "uv run make lint test dbt-parse docs-build docs-structure" in workflow_text
    assert "live-smoke" not in workflow_text
    assert "source-audit" not in workflow_text
    assert "docs-render" not in workflow_text
    assert not (workflow_path.parent / "live-readiness.yml").exists()
    assert sorted(path.name for path in workflow_path.parent.glob("*.yml")) == [
        "ci.yml",
        "docs.yml",
    ]


def test_docs_workflow_deploys_on_main_tags_and_dispatch():
    workflow_path = REPO_ROOT / ".github/workflows/docs.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    workflow_text = workflow_path.read_text(encoding="utf-8")
    trigger = workflow.get("on", workflow.get(True))
    assert trigger["push"]["branches"] == ["main"]
    assert trigger["push"]["tags"] == ["v*"]
    assert "workflow_dispatch" in trigger
    assert "pull_request" not in trigger
    assert workflow["permissions"]["contents"] == "write"
    assert "mkdocs gh-deploy" in workflow_text
    assert "timeout-minutes" in workflow_text
    assert (
        workflow["jobs"]["release-assets"]["if"]
        == "startsWith(github.ref, 'refs/tags/v')"
    )


def test_readme_links_to_canonical_hubs():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for term in (
        "docs/getting-started/index.md",
        "docs/guides/query-the-warehouse.md",
        "docs/reference/data-contracts.md",
        "docs/guides/day-two-operations.md",
        "docs/audiences/analysts.md",
        "(CONTRIBUTING.md)",
        "(PRIVACY.md)",
        "(THIRD_PARTY_NOTICES.md)",
    ):
        assert term in readme


def test_no_legacy_flat_docs_paths():
    documentation = _all_docs()
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for stale in (
        "docs/consumer-guide.md",
        "docs/operations.md",
        "docs/quickstart.md",
        "docs/architecture.md",
        "docs/legal.md",
        "docs/warehouse.md",
        "docs/data-contracts.md",
        "docs/configuration.md",
        "docs/development.md",
        "docs/troubleshooting.md",
        "docs/source-coverage.md",
        "docs/changelog.md",
        "docs/naming.md",
    ):
        assert stale not in readme
        assert stale not in documentation


def test_scripts_inventory_is_documented():
    scripts_doc = (DOCS / "reference/scripts.md").read_text(encoding="utf-8")
    scripts = sorted(
        path.name
        for path in (REPO_ROOT / "scripts").glob("*.py")
        if not path.name.startswith("_")
    )
    assert scripts
    for name in scripts:
        assert name in scripts_doc, name


def test_query_chooser_covers_public_marts():
    chooser = (DOCS / "guides/query-the-warehouse.md").read_text(encoding="utf-8")
    for mart in PUBLIC_MARTS:
        assert mart in chooser, mart


def _policy_corpus() -> str:
    parts = [path.read_text(encoding="utf-8") for path in sorted(DOCS.rglob("*.md"))]
    for name in ("README.md", "AGENTS.md", "CONTRIBUTING.md"):
        parts.append((REPO_ROOT / name).read_text(encoding="utf-8"))
    return "\n".join(parts)


def test_stale_phrase_denylist():
    corpus = _policy_corpus()
    for phrase in (
        "docs/legal.md",
        "produces a TravelCanary score",
        "TravelCanary recommendation score",
        "GitHub Actions runs live",
        "GitHub Actions run live",
    ):
        assert phrase not in corpus, phrase
