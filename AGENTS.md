# TravelCanary Pipeline — agent guide

Local-first `dlt` + `dbt` + `DuckDB` + `Dagster` pipeline for trustworthy country-level travel-risk signals.

## Principles

- Reuse existing helpers in this repo before adding dependencies.
- Prefer stdlib; keep diffs minimal.
- Breaking warehouse rebuilds remain allowed; preserve multi-day advisory history
  with `export-history` / `import-history` rather than compatibility aliases.
- Preserve native advisory meaning; normalized 1–4 levels are best-effort approximations.
- Never add a TravelCanary score or recommendation to public marts.
- Naming has **no scope token**: `<source>_<layer>_<subject>` (e.g. `us_state_raw_advisories`).

## Quality gate

```bash
make lint
make test-cov
make contract-http
make integration-dagster-cov
make integration-dbt-cov
make dbt-unit
make golden-dbt
make coverage-report
make docs-check
make check-secrets
make dbt-parse
make dbt-build-ci
make costguard
```

This is the full local release gate. GitHub Actions intentionally uses one
runner for less than five cumulative minutes and runs lint, fast offline tests
(including saved HTTP contracts), dbt parse, and a strict documentation build.
The full 100%-coverage, Dagster/dbt integration, golden, build, and Costguard
checks remain mandatory before a release. Live audits and ingestion are
local-only.

## Layout

- `src/travelcanary_pipeline/ingestion/` — per-source fetch/parse/sync
- `src/travelcanary_pipeline/storage/duckdb/` — warehouse connection and schemas
- `src/travelcanary_pipeline/orchestration/` — Dagster assets, jobs, schedules
- `src/travelcanary_pipeline/export.py` — public mart Parquet export
- `src/travelcanary_pipeline/history_transfer.py` — history export/re-import
- `dbt/` — staging → intermediate → marts → observability
- `tests/` — unit, integration, dbt policy tests

## Sources (v0.4.0)

Official adapters: US, Canada, UK, Netherlands, Japan.
Required context: complete GDELT 1 daily Events export.

Regional public models, scoring, APIs, GDELT 2, and additional issuers are out of scope.
