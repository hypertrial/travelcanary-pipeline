# Data contracts

`dbt/seeds/source_contracts.csv` is authoritative for both Python ingestion and dbt observability. Explicit allowed normalization gaps live in `dbt/seeds/advisory_normalization_exceptions.csv` and remain visible in observability. Do not duplicate thresholds or exceptions in adapter code.

## Guard policy

- Static row floor: 80% of a verified healthy complete-catalog live audit.
- Relative size: warning below 90% of the previous accepted count; rejection below 80%.
- Freshness: warning after 36 hours without acceptance; error/stale after 72 hours.
- Official canonical ISO coverage: rejection below 98%; warning below 99%.
- Official normalization: at least 99% of rows with non-null native levels, except documented source-native cases.
- GDELT: rejection below 95% mapped non-empty action-country codes; warning below 98%.

Every source is required. A failure stops the full pipeline. A run is accepted only after its raw write commits, and staging joins raw records to accepted run IDs.

Official batches must also have unique `advisory_id` business keys before landing. Candidate rows are appended first; a single DuckDB transaction then records the run as accepted and prunes every other raw run ID. If that transaction fails, neither acceptance nor pruning commits, so the previous accepted batch remains visible. GDELT consumes and evaluates its streamed rows inside the upsert transaction; rejection, parsing failure, or write failure rolls back all raw mutations before recording the outcome.

Adapters count every discovered, skipped, and blocking-skipped source record.
Non-object advisories, missing document identities, invalid XML/pages, and
structurally malformed GDELT rows reject required batches before acceptance.
The only benign skip is a withdrawn UK advice page returning HTTP
404; accepted batches record its categorized count in the existing ledger
message. Transport failures remain errors rather than parse skips.

The Python contract loader and dbt tests also validate the seed itself before
use: the exact registry-derived six required sources, positive counts and
freshness windows, ratios within `[0, 1]`, ordered warning/rejection ratios,
ordered canonical thresholds, and warning freshness strictly below error
freshness. Invalid or duplicate rows fail with their source and field instead
of silently replacing a contract.

## Offline regression contracts

Sanitized saved payload fragments exercise the real fetch/parse entry point
for all five official adapters and GDELT. They preserve
provider nesting and field names while excluding full catalogs, credentials,
headers, and long prose. These network-free checks run in the default coverage
gate and are available directly as `uv run make contract-http`.

Reviewed CSV fixtures compare every column and row for five critical marts:
`country_travel_risk`, `country_risk_overview`,
`country_advisory_changes`, `country_context_alerts`, and
`source_data_quality`. Date/timestamp/elapsed-hour values are normalized;
nulls, booleans, numeric values, statuses, counts, text, and column order remain
exact. Schema and column-order validation continues to cover all ten public
marts. Run the focused regression with `uv run make golden-dbt`.

## Country identity

Advisories resolve in this order:

1. Explicit source-specific crosswalk.
2. Valid ISO3 lookup.
3. Valid ISO2 lookup.
4. Unresolved.

The crosswalk remains sparse and contains only genuine nonstandard identifiers. Public marts include only countries in `iso_countries.csv`; regional, compound, and unresolved records remain in observability with a reason.

## Advisory semantics

`country_travel_risk` exposes canonical ISO2/ISO3/name, issuer, UTC snapshot date, native level/label, normalized ordinal/label, normalization status, summary, source URL, safely parsed source publication timestamp, and ingestion timestamp.

Normalization is an approximation for comparison. Null native levels remain null, and unmapped values remain visible rather than being assigned a safe value.

## Transparent signals

`country_risk_signals` exposes reporting issuer count; minimum, median, maximum, and range of normalized levels; latest official ingestion; and GDELT one-/seven-day event statistics and freshness. It contains no combined TravelCanary score or recommendation.

`country_risk_overview` is the current analyst entry point at one row per ISO3.
It combines selected public signals, trend and change counts, sorted theme and
alert names, and explicit source-usability counts and flags. Missing aggregate
counts become zero, boolean aggregates become false, and absent multi-value
text remains null. Its health fields describe pipeline data usability, never
destination safety. Detailed native values, matched terms, alert reasons, and
source URLs remain in their dedicated marts.

## Insight marts

`country_advisory_changes` compares each issuer's current daily advisory row with its previous accepted daily snapshot and labels the movement as `new`, `worsening`, `improving`, `unchanged`, or `unknown`.

`country_risk_trends` aggregates official history by country and UTC snapshot date. Direction is based on median normalized official advisory level; issuer disagreement is based on the normalized range.

`country_advisory_themes` uses the auditable `advisory_theme_keywords` seed to flag five advisory text themes: terrorism, health, crime, conflict, and natural disasters. It stores matched keyword counts and matched terms, not NLP classifications.

`country_gdelt_event_types` aggregates accepted GDELT country events by daily CAMEO root event code and exposes event count, mention count, material-conflict count/share, average Goldstein scale, and average tone.

`country_context_alerts` contains explainable official-versus-GDELT divergence flags only. Missing or stale GDELT suppresses alerts and remains visible through `source_data_quality`.

`source_data_quality` promotes source health, freshness, row-volume, canonical coverage, and normalization status from observability into a consumer-facing mart.

## History transfer policy

`make export-history` / `make import-history` transfer only
`travelcanary_marts.country_travel_risk_history`. Import validates Parquet
columns against `PUBLIC_MART_COLUMNS` and inserts rows whose unique key
`(destination_iso3, issuing_government, snapshot_date)` is absent. Existing
warehouse rows win: a same-day corrected row is never displaced by an older
export. Change and trend marts regenerate from restored history on the next
dbt build.
