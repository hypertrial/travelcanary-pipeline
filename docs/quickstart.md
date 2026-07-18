# Quickstart

## Requirements

- Python 3.10 or newer
- [uv](https://docs.astral.sh/uv/)
- A local filesystem path writable by DuckDB
- Optional: the `duckdb` CLI on `PATH` for the local UI

## Install and validate offline

```bash
cp .env.example .env
uv sync --locked --extra dev
uv run make demo
uv run make demo-ui
```

`demo` removes and recreates only `.cache/travelcanary_demo.duckdb`, loads
deterministic data, runs dbt, and validates the public contract. `demo-ui` does
the same and then opens DuckDB's local UI. Neither command reads live sources
or an operator `DUCKDB_PATH`.

Contributors can run the offline quality checks separately:

```bash
uv run make lint
uv run make test
uv run make dbt-build-ci
uv run make docs-check
```

`dbt-build-ci` retains its CI-specific `.cache/dbt_build.duckdb` path.

## First live run

1. Run `uv run make source-audit`. This fetches and validates live sources without writing the operator warehouse.
2. Before deleting an existing warehouse, run `uv run make export-history` if
   you need to retain multi-day advisory history. Then delete the operator
   DuckDB file and rebuild; use `uv run make import-history` after the first
   successful rebuild when restoring history.
3. Start Dagster with `uv run make dagster-dev`.
4. Materialize `travelcanary_full_pipeline` manually and inspect `travelcanary_observability.source_health` plus all gap models.
5. Only after a successful manual run, set `TRAVELCANARY_DAILY_SCHEDULE_ENABLED=true` and restart Dagster if daily execution is wanted.

The built-in schedule runs at `08:00 UTC` and is stopped by default.

For targeted live diagnosis:

```bash
uv run python scripts/audit_live_sources.py --source jp_mofa --output json
uv run python scripts/audit_live_sources.py --source jp_mofa --warehouse travelcanary.duckdb --output json
uv run python scripts/audit_live_sources.py --source gdelt --date 2026-07-09 --output text
uv run python scripts/audit_live_sources.py --propose-floors
```

`--warehouse PATH` is optional and read-only. Use it when you want the live audit to compare a fetched batch with the latest accepted run in an existing warehouse; omit it for a pure network/source-contract check.

To exercise the full live Dagster path without touching the operator warehouse:

```bash
uv run make live-smoke
```

`live-smoke` always writes the warehouse to `.cache/live_smoke.duckdb` and keeps transient dbt/dlt state under `.cache/live_smoke_*`. It runs the full `source-audit` and stops before materialization when any required source is unavailable, rejected, or errored.

GitHub Actions never runs live-source audits or live ingestion. Keep these
network and warehouse checks on an operator-owned machine; capture only
sanitized command output when sharing diagnostics.

## Query the marts

```sql
select *
from travelcanary_marts.country_risk_overview
order by worsening_issuer_count desc, destination_iso3;
```

See the [consumer guide](consumer-guide.md) for analysis and drill-down queries.
Set `DUCKDB_PATH` when the operator warehouse should live outside the repository.
