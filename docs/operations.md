# Operations and recovery

## Daily operation

The one built-in Dagster schedule targets `travelcanary_full_pipeline` at `08:00 UTC` and is stopped unless `TRAVELCANARY_DAILY_SCHEDULE_ENABLED=true`. Perform a manual full materialization and inspect observability before enabling it.

The full job runs all five official catalogs, the required GDELT daily export,
and dbt. All timestamps, freshness windows, and snapshot dates use UTC.

The local Dagster instance queues one run at a time and executes assets
in-process. Supported writers also acquire `<warehouse>.writer.lock` without
waiting. If a direct command overlaps another writer, it fails with the active
warehouse path; wait for that writer to finish and retry.

## Batch outcomes

`travelcanary_ops.source_sync_runs` records `accepted`, `rejected`, or `error` with counts, ratios, timestamps, role, and a concise reason.

- A required rejection or error fails ingestion. dbt does not run, so existing public marts remain last-known-good.
- Rows from a write whose run was not finalized as accepted are ignored by staging.
- A successful official finalization retains only that accepted raw batch. If finalization fails after append, its orphaned rows may remain temporarily but cannot displace the prior accepted run.
- dbt builds in a same-directory candidate. A dbt model/test failure removes the candidate and retains the previous public marts, while accepted raw rows and ledger outcomes already committed to the primary warehouse remain available for the next build.

## Recovery

1. Query `travelcanary_observability.source_health` and the related gap model.
2. Run `uv run make source-audit` to distinguish upstream breakage from warehouse state. The audit never mutates the warehouse.
3. Correct the adapter, mapping, contract, or upstream availability issue; do not mark a suspect batch accepted by hand.
4. Rerun the failed ingestion job, then `travelcanary_dbt_build` or the full job. The next successful official finalization removes superseded and orphaned raw rows automatically.
5. Verify current marts and history before re-enabling the schedule.

For a dbt-only failure after successful ingestion, do not re-mark or reload the
accepted batch. Correct the transformation or data issue and rerun
`travelcanary_dbt_build`/`make dbt-build`; the candidate is rebuilt from the
accepted raw state and promoted only after the full dbt build passes. Stale
managed candidate files from an interrupted process are removed by the next
locked build. Reconnect DuckDB UI after a successful promotion because an open
client may still reference the replaced file's old inode.

Same-day reruns replace the current UTC snapshot in `country_travel_risk_history`. Older accepted snapshots are retained indefinitely in the mart, not in official raw storage. Official raw tables are current-batch-only after successful finalization; GDELT raw events are retained for `GDELT_ROLLING_WINDOW_DAYS`.

## Preserve history across rebuilds

`country_travel_risk_history` is the only multi-day official advisory store.
Raw tables keep the current accepted batch only, so a deleted warehouse loses
history unless you export it first.

```bash
uv run make export-history
# stop Dagster; delete the operator DuckDB file and WAL/lock siblings
uv sync --locked --extra dev
# run a clean full pipeline or offline demo rebuild
uv run make import-history HISTORY_PATH=exports/country_travel_risk_history.parquet
uv run make dbt-build
```

`export-history` writes a Parquet file plus a sibling
`<stem>.manifest.json`. `import-history` acquires the warehouse writer lock,
validates the Parquet columns against the public contract, creates the history
table when absent, and inserts only rows whose
`(destination_iso3, issuing_government, snapshot_date)` key is missing.
Existing warehouse rows win, so a same-day corrected row is never displaced by
an older export. Import against the primary warehouse only, never a dbt
candidate file. A later dbt `--full-refresh` of the history model wipes
imported rows; re-import after any full refresh.

## Upgrade to 0.5.0

A clean rebuild may still be required when schemas change. Before deleting the
operator warehouse, run `uv run make export-history`. After the rebuild and
first successful ingest/dbt build, run `import-history` and rebuild dbt so
change and trend marts regenerate from the restored history. Whole-file
backups remain valid but are no longer the only supported history bridge.
On memory-constrained hosts, set `DUCKDB_MEMORY_LIMIT` before the first live
GDELT sync.

## Live source audit cadence

Run `uv run make source-audit` at least weekly on an operator-owned machine,
and always before tagging a release. Treat a required-source rejection as a
release blocker until the adapter, mapping, contract, or upstream availability
issue is understood. Retain only reviewed sanitized audit output when sharing
diagnostics; GitHub Actions never runs live audits.

Before tagging a release, also run `uv run make live-smoke` on an
operator-owned machine. A live-smoke failure is a release blocker. Create the
GitHub release with `gh release create` (which pushes the `v*` tag) so the
tag-triggered documentation deploy and demo Parquet release-asset jobs attach
to an existing release.

## Live source audit

`source-audit` is the read-only operator probe. It fetches live source data, parses it, evaluates the committed source contract, and exits nonzero when any selected required source is not accepted.

Examples:

```bash
uv run make source-audit
uv run python scripts/audit_live_sources.py --source jp_mofa --output json
uv run python scripts/audit_live_sources.py --source jp_mofa --warehouse travelcanary.duckdb --output json
uv run python scripts/audit_live_sources.py --source us_state --source ca_gac --output text
uv run python scripts/audit_live_sources.py --source gdelt --date 2026-07-09
uv run python scripts/audit_live_sources.py --propose-floors
```

`--source` is repeatable and must match `dbt/seeds/source_contracts.csv`. JSON is the default output and is safe to redirect because progress is written to stderr. Each result includes start/finish timestamps, duration, role, row count, coverage ratios when applicable, minimum rows, reason, source URL, and `discovered_rows`, `skipped_rows`, `blocking_skipped_rows`, and deterministic `skip_reasons` parse diagnostics.

The executable script is a thin CLI over the reusable
`travelcanary_pipeline.live_audit` engine. It does not acquire the writer lock
and never mutates the operator warehouse.

`--warehouse PATH` is opt-in previous-run context. When provided, `source-audit` opens the DuckDB file read-only, reads the latest accepted `travelcanary_ops.source_sync_runs` row per selected source, and adds `previous_accepted_rows`, `previous_accepted_finished_at`, `relative_ratio`, and `previous_context_status`. Missing, locked, or older warehouses do not fail the audit; the result reports `previous_context_status=unavailable` and a concise reason. Without `--warehouse`, `source-audit` does not open or initialize any operator warehouse.

`--propose-floors` calculates 80% floors for accepted live counts and prints them in the audit output. It never edits `dbt/seeds/source_contracts.csv`; floor changes still require review evidence from a healthy full-catalog audit.

## Disposable live smoke

`uv run make live-smoke` is the mandatory pre-tag live readiness workflow on an
operator-owned machine. It ignores any operator `DUCKDB_PATH`, resets only
`.cache/live_smoke.duckdb` plus `.cache/live_smoke_*` transient dbt/dlt state,
runs the full `source-audit`, materializes the same asset path as
`travelcanary_full_pipeline`, and validates the resulting marts and source
health. On memory-constrained hosts, set `DUCKDB_MEMORY_LIMIT` (for example
`8GB`) before rerunning; GDELT uses a disk-backed native upsert path that no
longer depends on row-by-row `executemany`.

If a required source fails the audit, `live-smoke` stops before Dagster materialization. This is expected when the upstream source is unavailable; the operator warehouse is not touched.

## Local-only live readiness

GitHub Actions never runs live-source audits or live ingestion. The repository
uses one offline CI runner capped at five minutes total. `source-audit` and
`live-smoke` are required before tagging a release and must run only on an
operator-owned machine. Retain disposable state below `.cache/`, and share only
reviewed sanitized output when diagnosing a source.
