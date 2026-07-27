# Troubleshooting

Each section is symptom → diagnostic → fix. Prefer one warehouse writer at a
time. Schedules stay disabled unless you intentionally enable them.

## Writer lock / concurrent writer

**Symptom:** Dagster, dbt, history import, or a Make target fails because a
warehouse writer is already active (`writer.lock` / lock acquisition error).

**Diagnostic:** Identify competing writers (Dagster UI, another shell,
long-running Make). Supported writers acquire a sibling
`<warehouse>.writer.lock` next to the DuckDB file (default
`travelcanary.duckdb.writer.lock`).

**Fix:** Wait for the named operator workflow to finish, then retry. Do not
delete the lock file to interrupt an active process; the operating-system lock,
not the file's presence, determines ownership. Stop Dagster and open DuckDB UI
sessions if they hold a write connection.

## Atomic dbt publish blocked by open DuckDB sessions

**Symptom:** `make dbt-build` or Dagster dbt fails with an error that the
warehouse cannot be checkpointed while another DuckDB session is open.

**Diagnostic:** List notebook kernels, `duckdb` UI windows, and Python
processes that still hold `read_only` or write connections to the warehouse
file.

**Fix:** Close every connection to that DuckDB file, then retry. `writer.lock`
serializes TravelCanary writers only; DuckDB itself still requires exclusive
access for the atomic publish checkpoint.

## Required source reject before dbt

**Symptom:** Full pipeline stops before dbt. Existing marts stay unchanged.

**Diagnostic:**

```sql
select
    source,
    status,
    finished_at,
    fetched_rows,
    previous_accepted_rows,
    relative_ratio,
    canonical_ratio,
    normalization_ratio,
    message
from travelcanary_ops.source_sync_runs
order by finished_at desc
limit 20;
```

```bash
uv run make source-audit
# Narrow: uv run python scripts/audit_live_sources.py --source us_state --output json
```

**Fix:** Inspect the failed Dagster ingestion asset and the ledger `message`.
Restore source availability or correct the reject cause (floor, freshness,
canonical coverage, parse/transport). Rerun ingestion; dbt advances only after
every required source accepts.

## History import without dbt rebuild

**Symptom:** `country_travel_risk_history` has restored rows, but
`country_advisory_changes` / `country_risk_trends` still look thin or unchanged.

**Diagnostic:**

```sql
select
    count(*) as history_rows,
    count(distinct snapshot_date) as history_dates
from travelcanary_marts.country_travel_risk_history;

select
    count(*) as trend_rows,
    count(distinct snapshot_date) as trend_dates
from travelcanary_marts.country_risk_trends;

select
    count(*) as changes_with_previous
from travelcanary_marts.country_advisory_changes
where previous_snapshot_date is not null;
```

**Fix:** Import alone does not rebuild insight marts. Run `uv run make dbt-build`
after `make import-history HISTORY_PATH=...`. Re-check with the
[Verify history import](query-recipes.md#verify-history-import) recipes. A later
`--full-refresh` of the history model wipes imported rows until you re-import.

## GDELT null or stale

**Symptom:** Overview/signals show null GDELT fields, `gdelt_is_fresh` is false,
or context alerts are suppressed.

**Diagnostic:**

```sql
select *
from travelcanary_marts.source_data_quality
where source = 'gdelt';

select
    destination_iso3,
    gdelt_event_count_7d,
    gdelt_latest_event_date,
    gdelt_is_fresh,
    gdelt_source_usable
from travelcanary_marts.country_risk_overview
where not gdelt_is_fresh
   or not gdelt_source_usable
order by destination_iso3
limit 50;
```

```sql
select *
from travelcanary_observability.gdelt_country_code_gaps
order by unmapped_rows desc
limit 50;
```

Confirm `GDELT_ROLLING_WINDOW_DAYS` in `.env` and the latest accepted GDELT
`source_sync_runs` row.

**Fix:** Repair GDELT acceptance (network, parse, mapping floor), then rerun
GDELT ingest and dbt. Events without usable action-country codes remain in
observability and cannot contribute to country marts. Missing/stale GDELT
suppresses context alerts by design.

## Destination absent / crosswalk gaps

**Symptom:** An expected country is missing from public marts.

**Diagnostic:**

```sql
select *
from travelcanary_observability.country_crosswalk_gaps
order by unresolved_rows desc
limit 50;

select *
from travelcanary_observability.advisory_normalization_gaps
limit 50;
```

Also inspect the issuer's current raw/accepted advisory rows for regional or
compound identifiers.

**Fix:** Country marts include only ISO countries in `iso_countries.csv`.
Regional, compound, and unresolved identifiers stay in observability. Add a
sparse crosswalk entry only for genuine nonstandard identifiers, then rebuild
dbt. Do not invent scores for unresolved destinations.

## US State empty catalog path

**Symptom:** US State Department JSON catalog returns an empty list and the
adapter falls back (or both paths fail).

**Diagnostic:**

```bash
uv run python scripts/audit_live_sources.py --source us_state --output json
```

Inspect sanitized JSON and stderr. Offline source-contract fixtures cover both
the JSON catalog and the official RSS fallback
(`https://travel.state.gov/_res/rss/TAsTWs.xml`).

**Fix:** If the JSON catalog is empty, the adapter uses the RSS feed
automatically. If both paths fail, preserve the last accepted raw batch, fix
connectivity or upstream payload shape, and rerun. Do not lower the source
floor. Live-source diagnostics stay on an operator-owned machine.

## Candidate dbt failed, raw kept

**Symptom:** Ingestion accepted, but public marts did not update after dbt
failed on the candidate warehouse.

**Diagnostic:**

```sql
-- Ledger still shows accepted raw runs
select source, status, finished_at, fetched_rows, message
from travelcanary_ops.source_sync_runs
where status = 'accepted'
order by finished_at desc
limit 20;

-- Published marts remain at the previous successful publication
select source, is_healthy, freshness_status, message
from travelcanary_marts.source_data_quality
order by source;
```

Confirm the dbt error in the Dagster/dbt logs. Candidate files under the
warehouse directory are deleted on failure.

**Fix:** Fix the reported model/test or data issue, then rerun
`uv run make dbt-build` (or the Dagster dbt job). The next candidate starts from
accepted raw state. Previously published marts stay until promotion succeeds.
Reconnect an open DuckDB UI after promotion.

## GDELT or live-smoke runs out of memory

**Symptom:** DuckDB out-of-memory during GDELT upsert or `make live-smoke`.

**Fix:** Set `DUCKDB_MEMORY_LIMIT` (for example `8GB`) in `.env`, stop competing
writers, and rerun against the disposable warehouse. Do not raise the limit
above what the host can spare.

## dbt shows duplicate raw assets

**Symptom:** Dagster reports duplicate dbt source/raw assets.

**Fix:** Regenerate the manifest with `uv run make dbt-parse` and run
`uv run make integration-dagster`. Each dbt source must have Dagster metadata
matching `[source, raw, subject]` exactly.
