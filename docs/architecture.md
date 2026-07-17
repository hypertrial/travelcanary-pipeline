# Architecture and lineage

TravelCanary uses one operator-controlled local DuckDB warehouse. Required ingestion completes before dbt advances public marts.

```text
five official catalogs ─┐
GDELT 1 daily Events ───┴─> guarded raw runs ─> accepted staging ─> canonical/normalized models ─> marts
                                                                       │
                                                                       └─> observability
```

## Layers

1. **Fetch and parse.** Each attempt gets a UUID `source_run_id`. Official adapters parse complete catalogs; GDELT parses the exact GDELT 1 daily schema.
2. **Guard.** Python reads `dbt/seeds/source_contracts.csv`, evaluates row count and coverage, and validates official `advisory_id` uniqueness before landing. Rejected GDELT writes are rolled back before their outcome is recorded.
3. **Land and finalize.** Official dlt resources append a complete candidate batch. A DuckDB transaction then records acceptance and deletes every superseded raw run together. GDELT streams its ZIP member into 5,000-row transactional upserts, then prunes its rolling window and records acceptance in the same transaction.
4. **Select accepted data.** Official staging selects the latest accepted catalog. GDELT staging selects every accepted retained daily run so seven-day windows remain complete.
5. **Transform.** dbt clones the checkpointed warehouse to a same-directory candidate, resolves ISO identities, normalizes levels, aggregates GDELT by country/day, and builds and tests marts there.
6. **Publish.** A successful dbt build checkpoints and atomically replaces the primary DuckDB file. A failed build deletes the candidate, leaving accepted raw and ledger state in the primary file while preserving its previously published marts.

## Dagster lineage

Raw dbt sources declare hierarchical asset keys such as `us_state / raw / advisories`. These are the same keys materialized by the Python ingestion assets, so there is no disconnected `us_state_raw / advisories` duplicate. dbt staging models are real children of ingestion.

The registered jobs are:

- `travelcanary_official_advisories_ingest`
- `travelcanary_gdelt_ingest`
- `travelcanary_dbt_build`
- `travelcanary_full_pipeline`

A required official or GDELT failure prevents downstream dbt execution. If official finalization fails after append, the previous accepted batch remains authoritative; staging ignores the orphaned candidate and the next successful finalization removes it.

Dagster uses the in-process executor because every ingestion asset targets the
same DuckDB file. The configured local queued run coordinator permits one
active run. A sibling advisory lock also serializes every supported ingestion,
seed, and dbt writer across processes, including direct Make commands.
