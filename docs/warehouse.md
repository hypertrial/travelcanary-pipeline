# Warehouse

The default warehouse is `travelcanary.duckdb`; set `DUCKDB_PATH` to place it elsewhere.

## Raw and operational schemas

Each official source has `<source>_raw.advisories`; GDELT uses `gdelt_raw.events`. Every row has a UUID `source_run_id`. dlt metadata may add internal columns.

`travelcanary_ops.source_sync_runs` is the acceptance ledger. Raw data is inspectable, but staging only exposes run IDs finalized as accepted. Official candidates land append-first; acceptance and deletion of all other raw run IDs commit in one DuckDB transaction. A failed finalization can leave orphaned candidate rows, but staging ignores them and the next successful run removes them.

Supported writers acquire a sibling `<warehouse>.writer.lock`. dbt then copies
the checkpointed primary file to a uniquely named candidate in the same
directory. Only a completely successful `dbt build` is checkpointed and
promoted with an atomic file replacement. Transformation failure cannot expose
partially rebuilt marts; accepted raw/ledger changes stay in the primary file
for a later retry. The next build removes stale managed candidate artifacts.

## dbt schemas

- `travelcanary_staging`: typed raw records limited to accepted runs.
- `travelcanary_intermediate`: ISO resolution, level normalization, and GDELT country aggregates.
- `travelcanary_marts`: stable analyst-facing relations.
- `travelcanary_observability`: source health and explicit gaps.

## Public marts

| Relation | Grain | Purpose |
| --- | --- | --- |
| `country_travel_risk` | destination ISO3, issuer, snapshot date | Current accepted official advisory |
| `country_travel_risk_history` | destination ISO3, issuer, snapshot date | Indefinite corrected daily history |
| `country_risk_signals` | destination ISO3, snapshot date | Issuer consensus/disagreement plus separately labeled GDELT context |
| `country_risk_overview` | destination ISO3 | Current analyst-ready signals, movement, themes, alerts, and data-health summary |
| `country_advisory_changes` | destination ISO3, issuer, snapshot date | Daily issuer-level advisory movement |
| `country_risk_trends` | destination ISO3, snapshot date | Daily country median-risk direction and disagreement trend |
| `country_advisory_themes` | destination ISO3, issuer, snapshot date, theme | Keyword-based advisory themes |
| `country_gdelt_event_types` | destination ISO3, event date, GDELT root event code | GDELT event counts and mention volume by event type |
| `country_context_alerts` | destination ISO3, snapshot date, alert type | Explainable official-versus-GDELT divergence flags |
| `source_data_quality` | source | Consumer-facing source health, freshness, and completeness |

There is no automatic migration layer. Use `make export-history` /
`make import-history` to preserve `country_travel_risk_history` across a
clean rebuild, then rebuild dbt so change and trend marts regenerate.

## Retention

Official history in `country_travel_risk_history` is indefinite. After successful acceptance, each official raw advisory table retains only the current accepted batch. GDELT Events are pruned after each accepted sync to `GDELT_ROLLING_WINDOW_DAYS` (120 by default). Rejected or failed GDELT mutations roll back before their ledger outcome is recorded.

Portable Parquet exports of the ten public marts are available through
`make export-marts`. They are read-only snapshots of the published marts and do
not replace the operator warehouse.

The operator controls the local DuckDB file. Underlying source content and
derived-data rights remain governed by their respective source terms; control
of the file does not transfer those rights.
