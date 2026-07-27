# Data dictionary

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs. Start with
    [Query the warehouse](../guides/query-the-warehouse.md). Formal grains and
    guard policy live in [Data contracts](data-contracts.md). Column lists in
    `dbt/models/marts/marts.yml` remain authoritative for schema detail.

## Core semantics

- Public marts are country-only and resolve through the committed ISO dimension.
- Normalized ordinals are best-effort 1–4 approximations; inspect native labels.
- Overview health fields describe pipeline usability, never destination safety.
- There is no TravelCanary score or recommendation.
- Change and trend marts regenerate from restored history only after the next
  dbt build; importing Parquet alone does not refresh them.

## `country_risk_overview`

| Field guidance | Detail |
| --- | --- |
| Intended use | Current analyst briefing at one row per ISO3 |
| Grain | `destination_iso3` |
| Recommended filters | Start here; drill into dedicated marts for native text/URLs |
| Common mistakes | Treating `all_required_sources_usable`, `gdelt_source_usable`, or usability counts as destination safety; inventing a combined score from overview fields |

## `country_travel_risk`

| Field guidance | Detail |
| --- | --- |
| Intended use | Current accepted official advisory per issuer |
| Grain | destination ISO3, issuer, snapshot date |
| Recommended filters | Inspect `native_level_label`, `normalization_status`, `source_url` |
| Common mistakes | Ignoring null native levels (`native_level` / `native_level_label` stay null when the source has no level); treating `normalized_ordinal` as authoritative over native meaning |

## `country_travel_risk_history`

| Field guidance | Detail |
| --- | --- |
| Intended use | Indefinite corrected daily history |
| Grain | destination ISO3, issuer, snapshot date |
| Recommended filters | Same-day reruns replace the current UTC snapshot |
| Common mistakes | Expecting raw tables to retain multi-day history; assuming `make import-history HISTORY_PATH=...` alone refreshes change/trend marts without a following `make dbt-build` |

## `country_risk_signals`

| Field guidance | Detail |
| --- | --- |
| Intended use | Issuer consensus/disagreement plus labeled GDELT context |
| Grain | destination ISO3, snapshot date |
| Common mistakes | Combining GDELT and official medians into a proprietary score; reading null GDELT fields as “safe” rather than missing/stale context |

## `country_advisory_changes`

| Field guidance | Detail |
| --- | --- |
| Intended use | Issuer-level daily movement |
| Grain | destination ISO3, issuer, snapshot date |
| Directions | `new`, `worsening`, `improving`, `unchanged`, `unknown` |
| Common mistakes | Expecting multi-day change rows after a history import without rebuilding dbt; treating `unknown` as a TravelCanary recommendation |

## `country_risk_trends`

| Field guidance | Detail |
| --- | --- |
| Intended use | Country median-risk direction and disagreement trend |
| Grain | destination ISO3, snapshot date |
| Common mistakes | Querying trends immediately after history import without `make dbt-build`; expecting trend depth when restored history was never imported |

## `country_advisory_themes`

| Field guidance | Detail |
| --- | --- |
| Intended use | Auditable keyword themes |
| Grain | destination ISO3, issuer, snapshot date, theme |
| Common mistakes | Treating themes as NLP classifications or as safety warnings |

## `country_gdelt_event_types`

| Field guidance | Detail |
| --- | --- |
| Intended use | GDELT root-event counts and mention volume |
| Grain | destination ISO3, event date, GDELT root event code |
| Common mistakes | Reading GDELT as an independent safety recommendation |

## `country_context_alerts`

| Field guidance | Detail |
| --- | --- |
| Intended use | Explainable official-versus-GDELT divergence flags |
| Grain | destination ISO3, snapshot date, alert type |
| Common mistakes | Treating alerts as travel warnings; ignoring that missing/stale GDELT suppresses alerts |

## `source_data_quality`

| Field guidance | Detail |
| --- | --- |
| Intended use | Consumer-facing source health, freshness, and completeness |
| Grain | source |
| Recommended filters | `where not is_healthy` for diagnosis |
| Common mistakes | Interpreting `is_healthy` / `is_usable` as destination safety rather than pipeline usability |

See [Warehouse](warehouse.md) for schema ownership and retention.
