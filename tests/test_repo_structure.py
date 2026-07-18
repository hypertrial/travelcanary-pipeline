import re
import subprocess
from pathlib import Path

import yaml

from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts

ROOT = Path(__file__).resolve().parents[1]


def test_open_source_policy_and_template_files_exist():
    required = (
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "PRIVACY.md",
        "SECURITY.md",
        "THIRD_PARTY_NOTICES.md",
        "CODE_OF_CONDUCT.md",
        ".github/CODEOWNERS",
        ".github/dependabot.yml",
        ".github/PULL_REQUEST_TEMPLATE.md",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/documentation.yml",
    )
    for relative in required:
        assert (ROOT / relative).is_file(), relative


def test_community_policy_links_resolve():
    source_path = ROOT / "README.md"
    required_targets = {
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "PRIVACY.md",
        "SECURITY.md",
        "THIRD_PARTY_NOTICES.md",
    }
    targets = set(re.findall(r"\]\(([^)]+)\)", source_path.read_text()))
    assert required_targets <= targets
    for target in required_targets:
        assert (source_path.parent / target).resolve().is_file()


def test_development_docs_link_to_community_policies():
    development = (ROOT / "docs" / "development.md").read_text()
    base = "https://github.com/hypertrial/travelcanary-pipeline/blob/main/"
    for target in (
        "CHANGELOG.md",
        "CODE_OF_CONDUCT.md",
        "CONTRIBUTING.md",
        "SECURITY.md",
    ):
        assert f"]({base}{target})" in development


def test_github_issue_forms_are_valid_yaml():
    for path in (ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.yml"):
        form = yaml.safe_load(path.read_text())
        assert form["name"]
        assert form["body"]


def test_dependabot_policy_is_constrained():
    config = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    updates = {item["package-ecosystem"]: item for item in config["updates"]}

    assert config["version"] == 2
    assert set(updates) == {"uv", "github-actions"}
    for update in updates.values():
        assert update["directory"] == "/"
        assert update["schedule"]["interval"] == "weekly"
        assert update["open-pull-requests-limit"] == 3
        assert len(update["groups"]) == 1
        group = next(iter(update["groups"].values()))
        assert set(group["update-types"]) == {"minor", "patch"}

    assert updates["uv"]["ignore"] == [
        {"dependency-name": "dbt-core", "versions": [">=1.12"]}
    ]
    assert "ignore" not in updates["github-actions"]


def test_github_actions_are_pinned_to_full_commits():
    workflows = "\n".join(
        path.read_text() for path in (ROOT / ".github/workflows").glob("*.yml")
    )
    refs = re.findall(r"uses:\s+[^@\s]+@([^\s#]+)", workflows)
    assert refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in refs)
    assert "uv sync --locked" in workflows


def test_runtime_telemetry_is_disabled():
    dbt_project = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text())
    dagster_instance = yaml.safe_load((ROOT / "dagster_instance.yaml").read_text())
    dlt_config = (ROOT / ".dlt/config.toml").read_text()

    assert dbt_project["flags"]["send_anonymous_usage_stats"] is False
    assert dagster_instance["telemetry"]["enabled"] is False
    assert re.search(
        r"(?ms)^\[runtime\]\s*$.*^dlthub_telemetry\s*=\s*false\s*$",
        dlt_config,
    )


def test_package_and_dbt_project_versions_match():
    pyproject = (ROOT / "pyproject.toml").read_text()
    package_version = re.search(r'(?m)^version\s*=\s*"([^"]+)"\s*$', pyproject)
    assert package_version is not None
    version = package_version.group(1)

    init = (ROOT / "src/travelcanary_pipeline/__init__.py").read_text()
    assert f'__version__ = "{version}"' in init

    http = (ROOT / "src/travelcanary_pipeline/resources/http.py").read_text()
    assert f"TravelCanary/{version}" in http

    dbt_project = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text())
    assert dbt_project["version"] == version


def test_live_audit_engine_is_owned_by_the_package():
    engine = ROOT / "src/travelcanary_pipeline/live_audit.py"
    cli = (ROOT / "scripts/audit_live_sources.py").read_text()
    smoke = (ROOT / "scripts/run_live_smoke.py").read_text()

    assert engine.is_file()
    assert "from travelcanary_pipeline.live_audit import" in cli
    assert "from travelcanary_pipeline.live_audit import" in smoke
    assert "from scripts.audit_live_sources import" not in smoke


def test_export_and_history_engines_are_owned_by_the_package():
    assert (ROOT / "src/travelcanary_pipeline/export.py").is_file()
    assert (ROOT / "src/travelcanary_pipeline/history_transfer.py").is_file()
    export_cli = (ROOT / "scripts/export_public_marts.py").read_text()
    history_export_cli = (ROOT / "scripts/export_history.py").read_text()
    history_import_cli = (ROOT / "scripts/import_history.py").read_text()
    assert "from travelcanary_pipeline.export import" in export_cli
    assert "from travelcanary_pipeline.history_transfer import" in history_export_cli
    assert "from travelcanary_pipeline.history_transfer import" in history_import_cli
    assert "resolve_export_dir" in history_export_cli


def test_python_and_dbt_source_contract_policies_share_sources_and_invariants():
    sql = (ROOT / "dbt/tests/assert_source_contracts_valid.sql").read_text()
    contracts = load_source_contracts()

    for source, contract in contracts.items():
        assert f"('{source}', '{contract.role}')" in sql
    for expression in (
        "minimum_rows <= 0",
        "warn_after_hours <= 0",
        "error_after_hours <= 0",
        "warn_after_hours >= error_after_hours",
        "warn_drop_ratio not between 0 and 1",
        "reject_drop_ratio not between 0 and 1",
        "reject_drop_ratio > warn_drop_ratio",
        "minimum_canonical_ratio not between 0 and 1",
        "warn_canonical_ratio not between 0 and 1",
        "minimum_canonical_ratio > warn_canonical_ratio",
        "minimum_normalization_ratio not between 0 and 1",
    ):
        assert expression in sql


def test_public_source_matrix_covers_the_registry():
    notices = (ROOT / "THIRD_PARTY_NOTICES.md").read_text()
    contracts = load_source_contracts()

    assert set(contracts) == {
        "ca_gac",
        "gdelt",
        "jp_mofa",
        "nl_mfa",
        "uk_fcdo",
        "us_state",
    }
    for source in contracts:
        assert f"`{source}`" in notices
    for removed in ("au_smartraveller", "de_aa", "travel_advisory_info"):
        adapter_dir = ROOT / "src/travelcanary_pipeline/ingestion" / removed
        assert not any(
            path.is_file() and "__pycache__" not in path.parts
            for path in adapter_dir.glob("**/*")
        )


def test_tracked_data_and_binary_policy():
    tracked = (
        subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=ROOT,
            check=True,
            capture_output=True,
        )
        .stdout.decode()
        .split("\0")
    )
    tracked_paths = [Path(path) for path in tracked if path and (ROOT / path).is_file()]
    forbidden_suffixes = {
        ".7z",
        ".db",
        ".dbf",
        ".duckdb",
        ".gz",
        ".key",
        ".nc",
        ".netcdf",
        ".p12",
        ".parquet",
        ".pem",
        ".pfx",
        ".shp",
        ".shx",
        ".sqlite",
        ".sqlite3",
        ".tar",
        ".tgz",
        ".zip",
    }
    assert not [
        str(path) for path in tracked_paths if path.suffix.lower() in forbidden_suffixes
    ]

    data_suffixes = {".csv", ".json", ".xml"}
    allowed_roots = (
        Path("dbt/seeds"),
        Path("src/travelcanary_pipeline/ingestion/jp_mofa"),
        Path("src/travelcanary_pipeline/ingestion/uk_fcdo"),
        Path("tests/fixtures"),
    )
    unexpected_data = [
        str(path)
        for path in tracked_paths
        if path.suffix.lower() in data_suffixes
        and not any(path.is_relative_to(root) for root in allowed_roots)
    ]
    assert not unexpected_data
    assert (ROOT / "tests/fixtures/SYNTHETIC.md").is_file()
