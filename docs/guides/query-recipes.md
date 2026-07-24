# Query recipes

Copy-paste SQL for common analyst paths. Open the demo warehouse with
`uv run make demo` or an operator DuckDB file. These recipes cover every public
mart in `PUBLIC_MARTS`. TravelCanary does **not** produce a safety score or
travel recommendation.

## Country briefing

`country_risk_overview` is one current row per ISO3. It assembles transparent
signals without producing a TravelCanary score or recommendation.

```sql
select *
from travelcanary_marts.country_risk_overview
where destination_iso3 = 'THA';
```

## Transparent risk signals

Issuer consensus and labeled GDELT context live in `country_risk_signals`. Do
not combine these fields into a proprietary score:

```sql
select
    destination_iso3,
    destination_name,
    reporting_issuer_count,
    normalized_ordinal_min,
    normalized_ordinal_median,
    normalized_ordinal_max,
    normalized_ordinal_range,
    gdelt_event_count_1d,
    gdelt_event_count_7d,
    gdelt_material_conflict_share_7d,
    gdelt_latest_event_date,
    gdelt_is_fresh
from travelcanary_marts.country_risk_signals
where destination_iso3 = 'THA';
```

## Find worsening issuers

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

Overview health fields make basic filtering join-free. They describe pipeline
usability, never destination safety:

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

## Inspect native meaning

Normalized levels are best-effort approximations. Before interpreting a
country, inspect every issuer's native label and official page. Null natives
stay null:

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

## Historical movement

```sql
select
    snapshot_date,
    issuing_government,
    normalized_ordinal,
    native_level_label
from travelcanary_marts.country_travel_risk_history
where destination_iso3 = 'THA'
order by snapshot_date desc, issuing_government
limit 40;
```

```sql
select *
from travelcanary_marts.country_risk_trends
where destination_iso3 = 'THA'
order by snapshot_date desc
limit 30;
```

## Verify history import

After `make import-history`, run `make dbt-build` so change and trend marts
regenerate from restored history. Then verify history depth and that trends
exist:

```sql
select
    count(*) as history_rows,
    count(distinct snapshot_date) as distinct_snapshot_dates,
    min(snapshot_date) as earliest_snapshot,
    max(snapshot_date) as latest_snapshot
from travelcanary_marts.country_travel_risk_history;
```

```sql
select
    count(*) as trend_rows,
    count(distinct snapshot_date) as distinct_trend_dates,
    min(snapshot_date) as earliest_trend,
    max(snapshot_date) as latest_trend
from travelcanary_marts.country_risk_trends;
```

```sql
select
    count(*) as change_rows_with_previous
from travelcanary_marts.country_advisory_changes
where previous_snapshot_date is not null;
```

## GDELT event types

```sql
select
    event_date,
    event_root_code,
    event_count,
    mention_count,
    material_conflict_share
from travelcanary_marts.country_gdelt_event_types
where destination_iso3 = 'THA'
order by event_date desc, event_count desc
limit 50;
```

Use `country_travel_risk_history`, `country_advisory_changes`, and
`country_risk_trends` for historical analysis. The overview contains current
state only. See the [Data dictionary](../reference/data-dictionary.md) for
grain and common mistakes.
