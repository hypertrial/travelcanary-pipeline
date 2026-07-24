# Query the warehouse

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs. Start here, then use
    [Query recipes](query-recipes.md), the
    [Data dictionary](../reference/data-dictionary.md), and
    [Data contracts](../reference/data-contracts.md).

TravelCanary's public marts are local DuckDB tables. Start with the current
country overview, then drill into native advisories and detailed context.
TravelCanary does **not** produce a safety score or travel recommendation.

## Build and open the offline demo

The demo is deterministic, uses no live sources, and always recreates only
`.cache/travelcanary_demo.duckdb`:

```bash
uv run make demo
```

If the optional `duckdb` CLI is installed and on `PATH`, build and open the
same warehouse in DuckDB's local UI:

```bash
uv run make demo-ui
```

## Table chooser

| Goal | Relation |
| --- | --- |
| Current country briefing | `travelcanary_marts.country_risk_overview` |
| Native issuer advisories | `travelcanary_marts.country_travel_risk` |
| Corrected daily history | `travelcanary_marts.country_travel_risk_history` |
| Consensus / disagreement + GDELT context | `travelcanary_marts.country_risk_signals` |
| Issuer movement | `travelcanary_marts.country_advisory_changes` |
| Country trends | `travelcanary_marts.country_risk_trends` |
| Keyword themes | `travelcanary_marts.country_advisory_themes` |
| GDELT event-type counts | `travelcanary_marts.country_gdelt_event_types` |
| Official vs GDELT flags | `travelcanary_marts.country_context_alerts` |
| Source health | `travelcanary_marts.source_data_quality` |

Prefer `country_risk_overview` for join-free filtering, then open dedicated
marts for native labels, URLs, matched terms, and alert reasons.

## Trust rules

- Normalized ordinals are best-effort approximations; inspect native labels.
- Overview health fields describe pipeline usability, never destination safety.
- Prefer `source_data_quality` when diagnosing missing or stale required
  sources.
- Do not treat `*_raw`, staging, or intermediate schemas as APIs.

Continue with [Query recipes](query-recipes.md).
