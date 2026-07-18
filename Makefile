.PHONY: dagster-dev duckdb-ui demo demo-ui _build-seeded-warehouse dbt-build dbt-build-ci dbt-parse dbt-test dbt-unit golden-dbt contract-http docs-serve docs-build docs-check source-audit live-smoke export-marts export-history import-history check-costguard-version costguard format lint test test-cov coverage coverage-erase coverage-report unit-ingest unit-orchestration integration-dbt integration-dbt-cov integration-dagster integration-dagster-cov check-secrets clean-local-artifacts

REPO_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
override PYTHON := $(shell if test -x "$(REPO_ROOT)/.venv/bin/python"; then printf '%s' "$(REPO_ROOT)/.venv/bin/python"; else printf 'python3'; fi)
RUN_IN_REPO := cd "$(REPO_ROOT)" &&
DUCKDB_NAME ?= travelcanary.duckdb
COSTGUARD_VERSION := 2.5.0
DBT_LINT_DUCKDB_PATH := $(REPO_ROOT)/.cache/dbt_lint.duckdb
DBT_LINT_ENV := DUCKDB_PATH="$(DBT_LINT_DUCKDB_PATH)"
DBT_BUILD_DUCKDB_PATH := $(REPO_ROOT)/.cache/dbt_build.duckdb
DEMO_DUCKDB_NAME := .cache/travelcanary_demo.duckdb
DEMO_DUCKDB_PATH := $(REPO_ROOT)/$(DEMO_DUCKDB_NAME)
SEEDED_DUCKDB_PATH = $(DBT_BUILD_DUCKDB_PATH)
SEEDED_BUILD_ENV = DUCKDB_NAME="$(SEEDED_DUCKDB_PATH)" DUCKDB_PATH="$(SEEDED_DUCKDB_PATH)"
DBT_UNIT_DUCKDB_PATH := $(REPO_ROOT)/.cache/dbt_unit.duckdb
DBT_UNIT_ENV := DUCKDB_NAME="$(DBT_UNIT_DUCKDB_PATH)" DUCKDB_PATH="$(DBT_UNIT_DUCKDB_PATH)"
PYTEST_FAST_MARKERS := not integration and not slow and not repo_check
PYTEST_COVERAGE_MARKERS := not slow and not repo_check
COV_APPEND_ARGS := --cov=travelcanary_pipeline --cov-branch --cov-append

duckdb-ui:
	@command -v duckdb >/dev/null 2>&1 || { printf '%s\n' 'duckdb CLI is required; install it and ensure it is on PATH.' >&2; exit 1; }
	duckdb "$(REPO_ROOT)/$(DUCKDB_NAME)" -ui

demo: SEEDED_DUCKDB_PATH := $(DEMO_DUCKDB_PATH)
demo: _build-seeded-warehouse

demo-ui: demo
	$(MAKE) duckdb-ui DUCKDB_NAME="$(DEMO_DUCKDB_NAME)"

dagster-dev:
	mkdir -p "$(REPO_ROOT)/.dagster_home"
	cp "$(REPO_ROOT)/dagster_instance.yaml" "$(REPO_ROOT)/.dagster_home/dagster.yaml"
	cd "$(REPO_ROOT)" && \
		export PATH="$(REPO_ROOT)/.venv/bin:$$PATH" && \
		export DAGSTER_HOME="$(REPO_ROOT)/.dagster_home" && \
		if test -x "$(REPO_ROOT)/.venv/bin/dg"; then \
			"$(REPO_ROOT)/.venv/bin/dg" dev -h 127.0.0.1 -w "$(REPO_ROOT)/workspace.yaml"; \
		else \
			"$(PYTHON)" -m dagster dev -h 127.0.0.1 -w "$(REPO_ROOT)/workspace.yaml"; \
		fi

dbt-build:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/run_atomic_dbt_build.py build

dbt-test:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/run_atomic_dbt_build.py test

dbt-build-ci: SEEDED_DUCKDB_PATH := $(DBT_BUILD_DUCKDB_PATH)
dbt-build-ci: _build-seeded-warehouse

_build-seeded-warehouse:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(SEEDED_BUILD_ENV) "$(PYTHON)" scripts/seed_ci_warehouse.py
	$(RUN_IN_REPO) $(SEEDED_BUILD_ENV) $(MAKE) dbt-build
	$(RUN_IN_REPO) $(SEEDED_BUILD_ENV) "$(PYTHON)" scripts/validate_ci_warehouse.py

dbt-parse:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt/profiles

dbt-unit:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(DBT_UNIT_ENV) "$(PYTHON)" scripts/run_dbt_unit.py

golden-dbt:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb/test_golden_marts.py -q -n 0 -m "not slow"

contract-http:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/contract -q -n 0 -m "contract"

docs-serve:
	$(RUN_IN_REPO) "$(PYTHON)" -m mkdocs serve

docs-build:
	$(RUN_IN_REPO) "$(PYTHON)" -m mkdocs build --strict

docs-check: docs-build
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/test_docs_structure.py -q -n 0

source-audit:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/audit_live_sources.py

live-smoke:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/run_live_smoke.py

export-marts:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/export_public_marts.py

export-history:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/export_history.py

import-history:
	@test -n "$(HISTORY_PATH)" || { printf '%s\n' 'HISTORY_PATH is required, e.g. make import-history HISTORY_PATH=exports/country_travel_risk_history.parquet' >&2; exit 1; }
	$(RUN_IN_REPO) "$(PYTHON)" scripts/import_history.py "$(HISTORY_PATH)"

check-costguard-version:
	@command -v costguard >/dev/null 2>&1 || { printf '%s\n' 'Costguard $(COSTGUARD_VERSION) is required. Install it with: curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v$(COSTGUARD_VERSION)' >&2; exit 1; }
	@version="$$(costguard --version 2>/dev/null || true)"; test "$$version" = "costguard $(COSTGUARD_VERSION)" || { printf '%s\n' "Costguard $(COSTGUARD_VERSION) is required; found $${version:-unknown}. Install it with: curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v$(COSTGUARD_VERSION)" >&2; exit 1; }

costguard: check-costguard-version
	$(RUN_IN_REPO) "$(PYTHON)" -m dbt.cli.main compile --project-dir dbt --profiles-dir dbt/profiles
	$(RUN_IN_REPO) costguard scan --warehouse duckdb --manifest dbt/target/manifest.json --fail-on high --min-confidence high dbt/models

format:
	$(RUN_IN_REPO) ruff format src tests scripts
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt/profiles
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m sqlfluff fix dbt/models dbt/tests

lint:
	$(RUN_IN_REPO) ruff format --check src tests scripts
	$(RUN_IN_REPO) ruff check src tests scripts
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m sqlfluff lint dbt/models dbt/tests -p 0
	$(MAKE) check-secrets

check-secrets:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/test_secrets_not_committed.py -q -n 0

test:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests -q -m "$(PYTEST_FAST_MARKERS)"

coverage-erase:
	$(RUN_IN_REPO) "$(PYTHON)" -m coverage erase

test-cov: coverage-erase
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests --ignore=tests/integration --ignore=tests/dbt -q -n auto -m "$(PYTEST_FAST_MARKERS)" $(COV_APPEND_ARGS)

coverage:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests -q -m "$(PYTEST_COVERAGE_MARKERS)" --cov=travelcanary_pipeline --cov-branch --cov-report=term-missing --cov-fail-under=100

coverage-report:
	$(RUN_IN_REPO) "$(PYTHON)" -m coverage report --show-missing --fail-under=100

unit-ingest:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/unit/ingestion -q -n 0 -m "$(PYTEST_FAST_MARKERS)"

unit-orchestration:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/unit/orchestration -q -n 0 -m "not slow"

integration-dbt:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb tests/dbt -q -n 0 -m "not slow"

integration-dbt-cov:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb tests/dbt -q -n 0 -m "not slow" $(COV_APPEND_ARGS)

integration-dagster:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster -q -n 0 -m "not slow"

integration-dagster-cov:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster -q -n 0 -m "not slow" $(COV_APPEND_ARGS)

clean-local-artifacts:
	$(RUN_IN_REPO) find . -type d -name __pycache__ -prune -exec rm -rf {} +
	$(RUN_IN_REPO) rm -rf .pytest_cache .ruff_cache .dagster_home .cache .coverage dbt/logs dbt/target site src/travelcanary_pipeline.egg-info exports
	$(RUN_IN_REPO) find . -maxdepth 2 \( -name '*.duckdb' -o -name '*.duckdb.tmp' -o -name '*.duckdb-wal' -o -name '*.duckdb-shm' -o -name '*.duckdb.wal' -o -name '*.duckdb.writer.lock' \) -exec rm -rf {} +
