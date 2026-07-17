# Changelog

## 0.3.0 — Public-readiness release — Unreleased

This is the planned first public release. Earlier releases and history are
private and unavailable. Existing warehouses require a clean rebuild.

- Consolidated GitHub Actions into one offline runner capped at five minutes
  total and moved every live audit/ingestion path to operator-owned local
  execution.
- Removed the German Federal Foreign Office, Australia Smartraveller, and
  travel-advisory.info vertical slices, including every `tai_*` public field.
  Australia failed the supported structured-endpoint and automated-reuse gate;
  no HTML scraping was substituted.
- Preserved Australia and Germany as destinations in retained-source and ISO
  mappings.
- Switched GDELT ZIP downloads to the HTTPS Google Storage endpoint and added
  public source/licence, privacy, DCO, attribution, no-endorsement, and
  no-travel-advice notices.

## 0.2.0 — Trustworthy local travel-risk pipeline — 2026-07-17

Breaking release. Existing warehouses must be reset.

- Added default-CI saved-payload contracts for all external adapters and exact
  seeded golden rows for five critical public marts.
- Fixed current Australia and Germany payload compatibility and added the
  official U.S. State Department RSS/XML fallback for an empty JSON catalog.
- Added constrained weekly Dependabot updates for Python and GitHub Actions.
- Constrained `dbt-core` below 1.12 and excluded incompatible 1.12+ updates from
  Dependabot until stable `dagster-dbt` releases support that boundary.
- Added single-writer DuckDB coordination and atomic dbt publication, strict
  adapter diagnostics and source-contract validation, correct blank warehouse
  defaults, and a reusable package-owned live-audit engine.
- Made official and TAI batch finalization crash-safe, streamed GDELT download
  and writes, guaranteed dbt timeout escalation, rejected invalid startup
  configuration, and pinned local Costguard to `2.5.0`.
- Added the current `country_risk_overview` mart with transparent movement,
  theme, context, and data-health fields.
- Added a complete public mart data dictionary and offline analyst demo and
  query workflow.

- Replaced `country_composite_risk` with transparent `country_risk_signals` and changed public mart grains/columns.
- Added guarded `source_run_id` raw schemas and `travelcanary_ops.source_sync_runs`; only accepted runs become current.
- Completed canonical country resolution with a full ISO dimension and removed ISO2-as-ISO3 fallback behavior.
- Switched event context to the complete GDELT 1 daily export with full country mapping, 120-day retention, and weighted seven-day aggregates.
- Made TAI optional/non-blocking and replaced `TRAVEL_ADVISORY_INFO_VERIFY_SSL` with secure-default `TRAVEL_ADVISORY_INFO_ALLOW_INSECURE_TLS`.
- Replaced the Dagster command surface with five explicit jobs and repaired ingestion-to-dbt asset lineage.
- Corrected daily history reruns, added source health/gap observability, strict docs, read-only live audit, and a 100% Python package coverage gate.
- Removed unused `dagster-dlt`, Polars, PyArrow, tqdm, and runtime PyYAML dependencies.

There are no compatibility aliases, migrations, regional public marts, APIs, exports, additional issuers, GDELT 2 support, or TravelCanary score in this release.
