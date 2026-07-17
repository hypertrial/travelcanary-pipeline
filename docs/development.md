# Development

## Quality gate

The local Costguard gate requires the external `costguard` CLI at exactly
`2.5.0`. Install the pinned release with the official command:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

`make costguard` checks the version and reports the detected binary; it never
installs or upgrades Costguard automatically.

```bash
uv run make lint
uv run make test-cov
uv run make contract-http
uv run make integration-dagster-cov
uv run make integration-dbt-cov
uv run make dbt-unit
uv run make golden-dbt
uv run make coverage-report
uv run make docs-check
uv run make check-secrets
uv run make dbt-parse
uv run make dbt-build-ci
uv run make costguard
```

See the repository
[contributor guide](https://github.com/hypertrial/travelcanary-pipeline/blob/main/CONTRIBUTING.md),
[security policy](https://github.com/hypertrial/travelcanary-pipeline/blob/main/SECURITY.md),
[changelog](https://github.com/hypertrial/travelcanary-pipeline/blob/main/CHANGELOG.md),
and [code of conduct](https://github.com/hypertrial/travelcanary-pipeline/blob/main/CODE_OF_CONDUCT.md)
for contribution, release, and private-reporting guidance.

`uv run make source-audit` and `uv run make live-smoke` are local-only because
they perform live network requests. GitHub Actions never runs live-source
audits or live ingestion. `source-audit` is source-only by default;
`--warehouse PATH` adds read-only previous-run context from an existing DuckDB
file without mutating or initializing it. `live-smoke` writes its warehouse to
`.cache/live_smoke.duckdb`, keeps transient dbt/dlt state under
`.cache/live_smoke_*`, and validates the Dagster/dbt path against that
disposable warehouse.

## Fixture policy

Adapter tests use sanitized upstream-shaped fixtures: field names, nesting,
types, and edge cases must match the provider. The reviewed fragments in
`tests/fixtures/contracts/` contain one or two records, no credentials or raw
headers, and no long advisory prose or complete catalogs. Their injected
fetchers never use the network. `contract-http` selects them directly, while
the default `test` and `test-cov` gates include them automatically. Update a
saved payload only when a reviewed upstream compatibility change requires a
matching parser change.

GDELT fixtures must be independently shaped like the official 58-field daily
export, not generated from the production parser column list. The contract
test wraps the existing text fixture in ZIP in memory rather than tracking a
binary response.

Golden CSVs cover the complete ordered rows of `country_travel_risk`,
`country_risk_overview`, `country_advisory_changes`,
`country_context_alerts`, and `source_data_quality`. `integration-dbt-cov`
includes these checks and `golden-dbt` selects them directly. Dates,
timestamps, and elapsed hours are normalized; every other value is exact.
Review a golden diff as a public output change rather than refreshing it
automatically.

Tests cover saved upstream payload shapes, batch guard thresholds, parse-loss
diagnostics, append/finalization failure visibility, duplicate business keys,
catalog identity, canonical resolution, normalization, streamed GDELT
batching/rollback/retention/rollups, cross-process writer locking, atomic dbt
failure/promotion, dbt process shutdown, strict configuration and contract
semantics, same-day history correction, exact critical-mart rows,
required-source behavior, and Dagster asset lineage.

Do not bypass the writer lock in a supported command or point dbt directly at
the operator file. New warehouse-writing entry points must use the existing
lock, and dbt publication must use the existing candidate lifecycle.

Do not commit DuckDB/WAL files, dlt/dbt caches, coverage output, or the generated MkDocs `site/` directory.

Do not commit downloaded catalogs or live advisory/event payloads. Fixtures
must be minimal, synthetic equivalents that exercise only the required schema.

## Changing a contract

Change the authoritative seed, tests, and source-coverage documentation together. A lower static floor requires evidence from a healthy full-catalog live audit; it is not a workaround for an upstream regression.
