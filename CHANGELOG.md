# Changelog

All notable changes are recorded here.

## 0.3.0 — Public-readiness release — Unreleased

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
