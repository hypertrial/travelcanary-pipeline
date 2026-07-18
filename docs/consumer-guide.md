# Consumer guide

TravelCanary's public marts are local DuckDB tables. Start with the current
country overview, then drill into native advisories and detailed context before
using the data in analysis.

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

## Country briefing

`country_risk_overview` is one current row per ISO3. It assembles transparent
signals without producing a TravelCanary score or recommendation.

```sql
select *
from travelcanary_marts.country_risk_overview
where destination_iso3 = 'THA';
```

## Find worsening issuers

List the official issuers whose normalized posture worsened since their
preceding accepted snapshot:

```sql
select
    destination_iso3,
    destination_name,
    issuing_government,
    previous_normalized_ordinal,
    normalized_ordinal,
    source_url
from travelcanary_marts.country_advisory_changes
where advisory_direction = 'worsening'
order by destination_iso3, issuing_government;
```

## Review issuer disagreement

```sql
select
    destination_iso3,
    reporting_issuers,
    normalized_ordinal_min,
    normalized_ordinal_median,
    normalized_ordinal_max,
    normalized_ordinal_range,
    disagreement_direction
from travelcanary_marts.country_risk_overview
where has_high_disagreement
order by normalized_ordinal_range desc nulls last;
```

## Inspect advisory themes

Themes are auditable keyword matches, not NLP classifications:

```sql
select
    issuing_government,
    theme,
    matched_keyword_count,
    matched_terms
from travelcanary_marts.country_advisory_themes
where destination_iso3 = 'THA'
order by theme, issuing_government;
```

## Inspect context alerts

Context alerts are explainable official-versus-GDELT threshold matches, not
safety warnings:

```sql
select
    alert_type,
    severity,
    official_median_ordinal,
    official_disagreement_range,
    gdelt_event_count_7d,
    gdelt_material_conflict_share_7d,
    reason
from travelcanary_marts.country_context_alerts
where destination_iso3 = 'THA'
order by alert_type;
```

## Check source health

Overview health fields make basic filtering join-free:

```sql
select
    destination_iso3,
    required_source_count,
    usable_required_source_count,
    gdelt_source_usable
from travelcanary_marts.country_risk_overview
where not all_required_sources_usable;
```

Use the authoritative source-level mart for diagnosis:

```sql
select *
from travelcanary_marts.source_data_quality
where not is_healthy
order by role, source;
```

## Export public marts

After a successful dbt build (or `make demo`), export every public mart to
Parquet for portable analysis outside the repository tooling:

```bash
uv run make export-marts
uv run python scripts/export_public_marts.py --output-dir /tmp/travelcanary-exports
```

The default directory is `EXPORT_DIR` (`exports/` under the repository root).
Each mart becomes `<mart>.parquet`, and `manifest.json` records the package
version, UTC export timestamp, per-mart row counts, and contracted column
lists. Export opens the warehouse read-only and fails if a mart is missing or
its columns drift from `PUBLIC_MART_COLUMNS`. `make export-history` uses the
same `EXPORT_DIR` default (`country_travel_risk_history.parquet` plus a sibling
manifest). Generated Parquet files are gitignored; do not commit them.

## Inspect native meaning

Normalized levels are best-effort approximations. Before interpreting a
country, inspect every issuer's native label and official page:

```sql
select
    issuing_government,
    native_level,
    native_level_label,
    normalized_ordinal,
    normalization_status,
    summary_text,
    source_url
from travelcanary_marts.country_travel_risk
where destination_iso3 = 'THA'
order by issuing_government;
```

Use `country_travel_risk_history`, `country_advisory_changes`, and
`country_risk_trends` for historical analysis. The overview contains current
state only.
