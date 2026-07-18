# Troubleshooting

## Full pipeline stops before dbt

Inspect the failed Dagster ingestion asset and `travelcanary_ops.source_sync_runs`. A required source was unavailable, rejected, or failed to commit. Existing marts intentionally remain unchanged. Run the read-only source audit, fix the cause, and rerun.

## A warehouse writer is already active

Dagster, ingestion, seeded builds, and dbt share one fail-fast sibling lock.
Wait for the command named in the operator workflow to finish, then retry. Do
not delete the lock file to interrupt an active process; the operating-system
lock, not the file's presence, determines ownership.

## dbt failed after ingestion succeeded

The accepted raw rows and ledger outcome remain in the primary warehouse, but
public marts remain at their previous successful publication. Fix the reported
dbt model/test or data issue and rerun the dbt build. The next candidate starts
from the accepted raw state. Reconnect an open DuckDB UI after promotion.

## US State Department JSON catalog is empty

The adapter automatically uses the State Department's official
`https://travel.state.gov/_res/rss/TAsTWs.xml` feed when
`https://cadataapi.state.gov/api/TravelAdvisories` returns an empty list. Both
payload paths are covered by offline source-contract fixtures. If both paths
fail, preserve the last accepted raw batch and inspect the local source-audit
diagnostics instead of lowering the source floor.

For local diagnosis, run
`uv run python scripts/audit_live_sources.py --source us_state --output json`
and inspect the sanitized JSON and stderr output. GitHub Actions does not run
live-source diagnostics.

## A destination is absent

Check `country_crosswalk_gaps`, `advisory_normalization_gaps`, and raw source rows. Country marts exclude regional, compound, and unresolved identifiers by design.

## GDELT fields are null or stale

Check `gdelt_country_code_gaps`, the accepted GDELT run date, and `GDELT_ROLLING_WINDOW_DAYS`. GDELT joins action geography; events without a usable country remain observable but cannot contribute to a country mart.

## History did not change after a rerun

History merges the latest existing UTC snapshot date. Confirm the current run completed dbt and that the source row itself changed. When upgrading across a breaking release, export history first with `make export-history`, rebuild the warehouse, then `make import-history` and rerun dbt. A dbt full refresh of the history model removes imported rows until you re-import.

## dbt shows duplicate raw assets

Regenerate the manifest with `uv run make dbt-parse` and run `uv run make integration-dagster`. Each dbt source must have Dagster metadata matching `[source, raw, subject]` exactly.
