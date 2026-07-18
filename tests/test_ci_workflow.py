import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_ci_workflow_is_one_bounded_offline_runner():
    workflow_path = ROOT / ".github" / "workflows" / "ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text())
    workflow_text = workflow_path.read_text()

    assert set(workflow["jobs"]) == {"fast-gate"}
    assert workflow["jobs"]["fast-gate"]["timeout-minutes"] == 5
    assert "uv run make lint test dbt-parse docs-build" in workflow_text
    assert 'TRAVELCANARY_DAILY_SCHEDULE_ENABLED: "false"' in workflow_text
    assert "live-smoke" not in workflow_text
    assert "source-audit" not in workflow_text
    assert "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" in workflow_text
    assert sorted(path.name for path in workflow_path.parent.glob("*.yml")) == [
        "ci.yml",
        "docs.yml",
    ]


def test_docs_workflow_publishes_on_version_tags_only():
    workflow_path = ROOT / ".github" / "workflows" / "docs.yml"
    workflow = yaml.safe_load(workflow_path.read_text())
    workflow_text = workflow_path.read_text()
    trigger = workflow.get("on", workflow.get(True))
    assert trigger["push"]["tags"] == ["v*"]
    assert "pull_request" not in trigger
    assert workflow["permissions"]["contents"] == "write"
    assert "mkdocs gh-deploy" in workflow_text
    assert workflow["jobs"]["publish"]["timeout-minutes"] == 5


def test_costguard_is_pinned_in_the_local_release_gate_only():
    makefile = (ROOT / "Makefile").read_text()
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    development = (ROOT / "docs" / "development.md").read_text()
    make_version = re.search(
        r"^COSTGUARD_VERSION := (\S+)$", makefile, re.MULTILINE
    ).group(1)

    assert make_version == "2.5.0"
    assert f"`{make_version}`" in development
    assert "costguard" not in workflow.lower()
