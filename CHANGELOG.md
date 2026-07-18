# Changelog

All notable changes are recorded here.

## 0.5.0 — Live-ready at scale — 2026-07-18

Fixes GDELT live ingestion memory pressure so `make live-smoke` can complete on
typical operator machines, documents an audit-only issuer evaluation, and
publishes demo Parquet exports as release assets.

- Replaced GDELT row-by-row `executemany` upserts with a disk-backed CSV stage
  and a single native `INSERT OR REPLACE ... SELECT FROM read_csv` inside the
  existing accept/reject transaction.
- Added optional `DUCKDB_MEMORY_LIMIT` and set
  `preserve_insertion_order = false` on writable DuckDB sessions.
- Hardened the US State RSS fallback: prefer title/slug identity when a FIPS
  country tag conflicts, and collapse duplicate `advisory_id` rows (empty JSON
  catalog currently yields dual classic/AEM RSS entries) before landing.
- Removed duplicate Japan MOFA country-code mappings for USA and Samoa and
  collapse any remaining same-ISO3 rows before landing so country marts keep a
  single grain.
- Evaluated New Zealand SafeTravel, Ireland DFA, and France Conseils aux
  Voyageurs against the structured-endpoint and automated-reuse gate; documented
  fail verdicts in `docs/source-coverage.md` and adopted none.
- Extended the tag-triggered `docs.yml` workflow with a `release-assets` job that
  builds the offline demo warehouse, exports public marts, and uploads synthetic
  Parquet assets to the GitHub release.
- Made `make source-audit` and `make live-smoke` mandatory local pre-tag checks
  while keeping them out of GitHub Actions.
- Kept the `dbt-core <1.12` pin; current `dagster-dbt` (through 0.29.11) still
  requires that upper bound. Lift it when upstream support lands.

## 0.4.0 — Portable, durable evidence — 2026-07-18

Adds portable Parquet exports of the public marts and a first-class history
export/re-import path so operators can preserve accepted advisory history
across breaking warehouse rebuilds.

- Added `make export-marts` / `scripts/export_public_marts.py` to export all
  ten public marts to Parquet with a contract-validated `manifest.json`.
- Added `EXPORT_DIR` (default `exports/`) and gitignored the export directory.
- Added `make export-history` / `make import-history` for
  `country_travel_risk_history`. Import acquires the writer lock, validates
  columns, and inserts only missing unique keys so existing warehouse rows win.
- Documented weekly and pre-release `source-audit` cadence, the rebuild bridge
  procedure, and the existing-rows-win history transfer policy.
- Added a tag-triggered MkDocs GitHub Pages workflow (`docs.yml`) so version
  tags publish the documentation site without lengthening the PR CI gate.
- Kept the `dbt-core <1.12` pin; current `dagster-dbt` still requires that
  upper bound. Lift it when upstream support lands.

## 0.3.0 — Public-readiness release — 2026-07-18

This is the planned first public release. Earlier versions and repository
history remain in a private archive and are not publicly available. Existing
warehouses must be rebuilt; there is no migration.

- Consolidated GitHub Actions into one offline runner capped at five minutes
  total. Full 100%-coverage, Dagster/dbt integration, golden, build, and
  Costguard validation remain local release gates.
- Removed GitHub-hosted live audits and ingestion; `source-audit` and
  `live-smoke` remain operator-owned local commands.
- Removed the German Federal Foreign Office adapter and its ingestion, Dagster,
  dbt, mapping, fixture, test, and public-contract slices.
- Removed travel-advisory.info and every `tai_*` public field.
- Removed Australia Smartraveller after its release gate found no documented,
  supported complete structured endpoint with confirmed automated reuse terms;
  HTML scraping is not used.
- Kept Australia and Germany as destinations in ISO and retained-source
  mappings.
- Changed GDELT daily ZIP downloads to the certificate-valid HTTPS Google
  Storage endpoint.
- Added canonical source/licence, privacy, DCO, attribution,
  no-endorsement, and no-travel-advice notices for public distribution.

## 0.2.0 — Trustworthy local travel-risk pipeline — 2026-07-17

Breaking release. Existing warehouses must be reset. TravelCanary now guards
complete official catalogs, publishes transparent country-level signals, and
uses accepted source-run history without compatibility aliases or a composite
safety score.

- Added network-free saved-payload contracts for every source adapter and
  reviewed exact-row golden regressions for five critical public marts.
- Fixed live compatibility with the current Smartraveller list export and
  Germany AA metadata, and added the official State Department RSS/XML fallback
  used when its JSON catalog is empty.
- Added constrained weekly Dependabot updates for Python and GitHub Actions.
- Constrained `dbt-core` below 1.12 and excluded incompatible 1.12+ updates from
  Dependabot until stable `dagster-dbt` releases support that boundary.
- Serialized all supported DuckDB writers, made dbt mart publication atomic,
  rejected hidden parse loss and invalid source-contract semantics, corrected
  blank warehouse configuration, and moved live auditing into package code.
- Hardened ingestion with crash-safe append-first official and TAI acceptance,
  streamed and batched GDELT processing, bounded dbt process shutdown, strict
  startup configuration, and a local Costguard `2.5.0` preflight.
- Added the analyst-ready `country_risk_overview` mart, complete public column
  documentation, and deterministic offline `demo`/`demo-ui` workflows.
- Added configurable dbt full-refresh runs with enforced no-progress timeouts.
- Split CI into parallel coverage, dbt, documentation, and quality gates.
- Added dbt unit tests for advisory normalization and trend classification.
- Added contributor, security, conduct, issue, and pull-request guidance.
- Integrated Costguard into the required CI aggregate and refreshed manual
  live-readiness actions.
