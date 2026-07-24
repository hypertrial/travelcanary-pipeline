# Validate and recover

## Recovery checklist

1. Query `travelcanary_observability.source_health` and the related gap model.
2. Run `uv run make source-audit` to distinguish upstream breakage from
   warehouse state. The audit never mutates the warehouse.
3. Correct the adapter, mapping, contract, or upstream availability issue; do
   not mark a suspect batch accepted by hand.
4. Rerun the failed ingestion job, then `travelcanary_dbt_build` or the full
   job. The next successful official finalization removes superseded and
   orphaned raw rows automatically.
5. Verify current marts and history before re-enabling the schedule.

For a dbt-only failure after successful ingestion, do not re-mark or reload the
accepted batch. Correct the transformation or data issue and rerun
`travelcanary_dbt_build`/`make dbt-build`; the candidate is rebuilt from the
accepted raw state and promoted only after the full dbt build passes. Stale
managed candidate files from an interrupted process are removed by the next
locked build. Reconnect DuckDB UI after a successful promotion because an open
client may still reference the replaced file's old inode.

Same-day reruns replace the current UTC snapshot in
`country_travel_risk_history`. Older accepted snapshots are retained
indefinitely in the mart, not in official raw storage. Official raw tables are
current-batch-only after successful finalization; GDELT raw events are retained
for `GDELT_ROLLING_WINDOW_DAYS`.

## Live source audit

`source-audit` is the read-only operator probe. It fetches live source data,
parses it, evaluates the committed source contract, and exits nonzero when any
selected required source is not accepted.

```bash
uv run make source-audit
uv run python scripts/audit_live_sources.py --source jp_mofa --output json
uv run python scripts/audit_live_sources.py --source jp_mofa --warehouse travelcanary.duckdb --output json
uv run python scripts/audit_live_sources.py --source us_state --source ca_gac --output text
uv run python scripts/audit_live_sources.py --source gdelt --date 2026-07-09
uv run python scripts/audit_live_sources.py --propose-floors
```

`--source` is repeatable and must match `dbt/seeds/source_contracts.csv`. JSON
is the default output and is safe to redirect because progress is written to
stderr. Each result includes start/finish timestamps, duration, role, row
count, coverage ratios when applicable, minimum rows, reason, source URL, and
`discovered_rows`, `skipped_rows`, `blocking_skipped_rows`, and deterministic
`skip_reasons` parse diagnostics.

The executable script is a thin CLI over the reusable
`travelcanary_pipeline.live_audit` engine. It does not acquire the writer lock
and never mutates the operator warehouse.

`--warehouse PATH` is opt-in previous-run context. When provided, `source-audit`
opens the DuckDB file read-only, reads the latest accepted
`travelcanary_ops.source_sync_runs` row per selected source, and adds
`previous_accepted_rows`, `previous_accepted_finished_at`, `relative_ratio`,
and `previous_context_status`. Missing, locked, or older warehouses do not fail
the audit; the result reports `previous_context_status=unavailable` and a
concise reason. Without `--warehouse`, `source-audit` does not open or
initialize any operator warehouse.

`--propose-floors` calculates 80% floors for accepted live counts and prints
them in the audit output. It never edits `dbt/seeds/source_contracts.csv`;
floor changes still require review evidence from a healthy full-catalog audit.

## Disposable live smoke

`uv run make live-smoke` is the mandatory pre-tag live readiness workflow on an
operator-owned machine. It ignores any operator `DUCKDB_PATH`, resets only
`.cache/live_smoke.duckdb` plus `.cache/live_smoke_*` transient dbt/dlt state,
runs the full `source-audit`, materializes the same asset path as
`travelcanary_full_pipeline`, and validates the resulting marts and source
health. On memory-constrained hosts, set `DUCKDB_MEMORY_LIMIT` (for example
`8GB`) before rerunning; GDELT uses a disk-backed native upsert path that no
longer depends on row-by-row `executemany`.

If a required source fails the audit, `live-smoke` stops before Dagster
materialization. This is expected when the upstream source is unavailable; the
operator warehouse is not touched.

## Local-only live readiness

GitHub Actions never runs live-source audits or live ingestion. The repository
uses one offline CI runner capped at five minutes total. `source-audit` and
`live-smoke` are required before tagging a release and must run only on an
operator-owned machine. Retain disposable state below `.cache/`, and share only
reviewed sanitized output when diagnosing a source.

See [Troubleshooting](troubleshooting.md) for common failure modes.
