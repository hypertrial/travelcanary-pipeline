# Preserve history across rebuilds

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

## Upgrade notes

A clean rebuild may still be required when schemas change. Before deleting the
operator warehouse, run `uv run make export-history`. After the rebuild and
first successful ingest/dbt build, run
`make import-history HISTORY_PATH=exports/country_travel_risk_history.parquet`
and rebuild dbt so change and trend marts regenerate from the restored history.
Whole-file backups remain valid but are no longer the only supported history
bridge.
On memory-constrained hosts, set `DUCKDB_MEMORY_LIMIT` before the first live
GDELT sync.

See [Data contracts](../reference/data-contracts.md) for the history transfer
policy and [Export public marts](export-public-marts.md) for portable mart
snapshots.
