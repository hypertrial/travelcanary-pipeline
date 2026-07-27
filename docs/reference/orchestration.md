# Orchestration

## Dagster jobs

| Job | Role |
| --- | --- |
| `travelcanary_official_advisories_ingest` | Five official catalogs into guarded raw |
| `travelcanary_gdelt_ingest` | Required GDELT 1 daily Events |
| `travelcanary_dbt_build` | Candidate dbt build and atomic publish |
| `travelcanary_full_pipeline` | Official + GDELT + dbt in one graph |

A required official or GDELT failure prevents downstream dbt execution. If
official finalization fails after append, the previous accepted batch remains
authoritative; staging ignores the orphaned candidate and the next successful
finalization removes it.

## Asset keys

Raw dbt sources declare hierarchical asset keys such as
`us_state / raw / advisories`. These are the same keys materialized by the
Python ingestion assets, so there is no disconnected
`us_state_raw / advisories` duplicate. dbt staging models are real children of
ingestion.

## Executor and locking

Dagster uses the in-process executor because every ingestion asset targets the
same DuckDB file. The configured local queued run coordinator permits one
active run. A sibling advisory lock (`<warehouse>.writer.lock`) also
serializes every supported ingestion, seed, and dbt writer across processes,
including direct Make commands. Supported writers acquire the lock without
waiting; overlapping writers fail with the active warehouse path. Atomic dbt
publish still needs exclusive DuckDB access during checkpoint and candidate
copy: close notebooks, `duckdb` UI, and other `read_only` sessions before
`make dbt-build` or the Dagster dbt asset runs.

## Schedule

The one built-in Dagster schedule targets `travelcanary_full_pipeline` at
`08:00 UTC` and is stopped unless `TRAVELCANARY_DAILY_SCHEDULE_ENABLED=true`.
Perform a manual full materialization and inspect observability before enabling
it. See [Enable schedules](../guides/enable-schedules.md).
