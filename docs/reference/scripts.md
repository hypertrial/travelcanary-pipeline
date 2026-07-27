# Scripts

Prefer Make targets from
[AGENTS.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/AGENTS.md)
over ad-hoc script flags unless diagnosing a source. Every non-private
`scripts/*.py` basename appears below.

## `audit_live_sources.py`

| | |
| --- | --- |
| Purpose | Read-only live source audit (sanitized counts and contract checks) |
| When to run | Before tagging a release; when diagnosing a required-source reject |
| When not | Not in GitHub Actions; never as a substitute for warehouse mutation |
| Key flags | `--source` (repeatable), `--date`, `--output json\|text`, `--propose-floors`, `--warehouse` |
| Make | `make source-audit` |

## `check_docs_recipe_sql.py`

| | |
| --- | --- |
| Purpose | Execute fenced SQL in `docs/guides/query-recipes.md` against the demo warehouse |
| When to run | After editing query recipes; as part of `docs-check` |
| When not | Not a warehouse validation substitute; does not hit live sources |
| Key flags | None (paths are fixed) |
| Make | `make docs-recipe-smoke` |

## `export_history.py`

| | |
| --- | --- |
| Purpose | Export `country_travel_risk_history` to Parquet plus sibling manifest |
| When to run | Before deleting or rebuilding an operator warehouse that holds history |
| When not | Not for public mart bulk export (use `export_public_marts.py`) |
| Key flags | Optional destination path (default `EXPORT_DIR/country_travel_risk_history.parquet`) |
| Make | `make export-history` |

## `export_public_marts.py`

| | |
| --- | --- |
| Purpose | Export every public mart to Parquet with `manifest.json` |
| When to run | When shipping portable mart snapshots for offline consumers |
| When not | Not a history bridge across rebuilds |
| Key flags | `--output-dir` (default `EXPORT_DIR`) |
| Make | `make export-marts`; demo rebuilds use `make export-demo-marts` |

## `import_history.py`

| | |
| --- | --- |
| Purpose | Import history Parquet into the primary warehouse (existing rows win) |
| When to run | After a clean rebuild, before relying on change/trend depth |
| When not | Never against a dbt candidate file; import alone does not rebuild insight marts |
| Key flags | Required source Parquet path |
| Make | `make import-history HISTORY_PATH=...` then `make dbt-build` |

## `run_atomic_dbt_build.py`

| | |
| --- | --- |
| Purpose | Locked candidate dbt build/test and atomic publish on success |
| When to run | Via Make/Dagster when publishing marts from accepted raw |
| When not | Do not point it at disposable CI warehouses from ad-hoc shells |
| Key flags | Positional `build` (default) or `test` |
| Make | `make dbt-build`, `make dbt-test` |

## `run_dbt_unit.py`

| | |
| --- | --- |
| Purpose | Seed an isolated warehouse and run dbt unit tests under one writer lock |
| When to run | Local/release gate (`make dbt-unit`) |
| When not | Not for operator production warehouses |
| Key flags | None |
| Make | `make dbt-unit` |

## `run_live_smoke.py`

| | |
| --- | --- |
| Purpose | Disposable live Dagster → dbt → warehouse validation path |
| When to run | On an operator-owned machine before tagging a release |
| When not | Never wire into GitHub Actions; not a read-only audit |
| Key flags | None (uses `.cache/live_smoke.duckdb`) |
| Make | `make live-smoke` |

## `seed_ci_warehouse.py`

| | |
| --- | --- |
| Purpose | Seed a disposable DuckDB warehouse for CI/offline dbt builds |
| When to run | Via `make dbt-build-ci` / seeded Make wrappers |
| When not | Never against the operator production warehouse |
| Key flags | None (honors `DUCKDB_PATH` from the Make env) |
| Make | Invoked by `_build-seeded-warehouse` / `dbt-build-ci` |

## `validate_ci_warehouse.py`

| | |
| --- | --- |
| Purpose | Assert public mart shapes and offline CI warehouse invariants |
| When to run | After a seeded CI dbt build |
| When not | Not a live readiness check |
| Key flags | None |
| Make | Invoked by `make dbt-build-ci` |

## `validate_live_warehouse.py`

| | |
| --- | --- |
| Purpose | Validate the disposable live-smoke warehouse against live public-mart contracts |
| When to run | After `run_live_smoke.py` materializes the disposable warehouse |
| When not | Not for arbitrary operator warehouses unless you accept live-smoke expectations |
| Key flags | None (uses the active DuckDB path) |
| Make | Invoked by `make live-smoke` |
