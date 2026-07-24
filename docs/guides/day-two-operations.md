# Day-two operations

Use this guide after a successful first manual full pipeline. Schedules stay
disabled until materialization and observability look healthy.

## Daily cadence

1. Keep `TRAVELCANARY_DAILY_SCHEDULE_ENABLED=false` until you intentionally
   enable the schedule (see [Enable schedules](enable-schedules.md)).
2. Prefer one writer at a time. The local Dagster instance queues one run and
   writers acquire `<warehouse>.writer.lock` without waiting.
3. Inspect `travelcanary_observability.source_health` and related gap models
   after each successful run.
4. All timestamps, freshness windows, and snapshot dates use UTC.

## Batch outcomes

`travelcanary_ops.source_sync_runs` records `accepted`, `rejected`, or `error`
with counts, ratios, timestamps, role, and a concise reason.

- A required rejection or error fails ingestion. dbt does not run, so existing
  public marts remain last-known-good.
- Rows from a write whose run was not finalized as accepted are ignored by
  staging.
- A successful official finalization retains only that accepted raw batch. If
  finalization fails after append, its orphaned rows may remain temporarily but
  cannot displace the prior accepted run.
- dbt builds in a same-directory candidate. A dbt model/test failure removes
  the candidate and retains the previous public marts, while accepted raw rows
  and ledger outcomes already committed to the primary warehouse remain
  available for the next build.

## Live source audit cadence

Run `uv run make source-audit` at least weekly on an operator-owned machine,
and always before tagging a release. Treat a required-source rejection as a
release blocker until the adapter, mapping, contract, or upstream availability
issue is understood. Retain only reviewed sanitized audit output when sharing
diagnostics; GitHub Actions never runs live-source audits or live ingestion.

Before tagging a release, also run `uv run make live-smoke` on an
operator-owned machine. A live-smoke failure is a release blocker. Documentation
publishes on pushes to `main`, `workflow_dispatch`, and `v*` tags. Create the
GitHub release with `gh release create` (which pushes the `v*` tag) so the
tag-only demo Parquet release-asset job attaches to an existing release.

See [Validate and recover](validate-and-recover.md) for audit flags and
recovery steps, and
[Preserve history across rebuilds](preserve-history-across-rebuilds.md) before
deleting a warehouse.
