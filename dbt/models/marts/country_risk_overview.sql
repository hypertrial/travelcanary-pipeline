with issuers as (
    select
        destination_iso3,
        snapshot_date,
        string_agg(
            distinct issuing_government,
            ', ' order by issuing_government
        ) as reporting_issuers
    from {{ ref('country_travel_risk') }}
    group by 1, 2
),

changes as (
    select
        destination_iso3,
        snapshot_date,
        count(*) filter (where is_changed) as changed_issuer_count,
        count(*) filter (
            where advisory_direction = 'worsening'
        ) as worsening_issuer_count,
        count(*) filter (
            where advisory_direction = 'improving'
        ) as improving_issuer_count
    from {{ ref('country_advisory_changes') }}
    group by 1, 2
),

themes as (
    select
        destination_iso3,
        snapshot_date,
        count(distinct theme) as matched_theme_count,
        string_agg(distinct theme, ', ' order by theme) as matched_themes
    from {{ ref('country_advisory_themes') }}
    group by 1, 2
),

alerts as (
    select
        destination_iso3,
        snapshot_date,
        count(*) as context_alert_count,
        string_agg(
            distinct alert_type,
            ', ' order by alert_type
        ) as context_alert_types,
        bool_or(severity = 'warning') as has_warning_context_alert
    from {{ ref('country_context_alerts') }}
    group by 1, 2
),

quality as (
    select
        count(*) filter (where role = 'required') as required_source_count,
        count(*) filter (
            where role = 'required' and is_usable
        ) as usable_required_source_count,
        coalesce(
            bool_and(is_usable) filter (where role = 'required'),
            false
        ) as all_required_sources_usable,
        coalesce(
            bool_or(is_usable) filter (where source = 'gdelt'),
            false
        ) as gdelt_source_usable
    from {{ ref('source_data_quality') }}
),

assembled as (
    select
        s.destination_iso2,
        s.destination_iso3,
        s.destination_name,
        s.snapshot_date,
        s.reporting_issuer_count,
        i.reporting_issuers,
        s.normalized_ordinal_min,
        s.normalized_ordinal_median,
        s.normalized_ordinal_max,
        s.normalized_ordinal_range,
        s.latest_official_ingested_at,
        t.risk_direction,
        t.disagreement_direction,
        t.has_high_disagreement,
        h.matched_themes,
        a.context_alert_types,
        s.gdelt_event_count_1d,
        s.gdelt_material_conflict_events_1d,
        s.gdelt_material_conflict_share_1d,
        s.gdelt_event_count_7d,
        s.gdelt_material_conflict_events_7d,
        s.gdelt_material_conflict_share_7d,
        s.gdelt_latest_event_date,
        s.gdelt_is_fresh,
        q.required_source_count,
        q.usable_required_source_count,
        q.all_required_sources_usable,
        q.gdelt_source_usable,
        coalesce(c.changed_issuer_count, 0) as changed_issuer_count,
        coalesce(c.worsening_issuer_count, 0) as worsening_issuer_count,
        coalesce(c.improving_issuer_count, 0) as improving_issuer_count,
        coalesce(h.matched_theme_count, 0) as matched_theme_count,
        coalesce(a.context_alert_count, 0) as context_alert_count,
        coalesce(a.has_warning_context_alert, false) as has_warning_context_alert
    from {{ ref('country_risk_signals') }} as s
    inner join issuers as i
        on
            s.destination_iso3 = i.destination_iso3
            and s.snapshot_date = i.snapshot_date
    left join {{ ref('country_risk_trends') }} as t
        on
            s.destination_iso3 = t.destination_iso3
            and s.snapshot_date = t.snapshot_date
    left join changes as c
        on
            s.destination_iso3 = c.destination_iso3
            and s.snapshot_date = c.snapshot_date
    left join themes as h
        on
            s.destination_iso3 = h.destination_iso3
            and s.snapshot_date = h.snapshot_date
    left join alerts as a
        on
            s.destination_iso3 = a.destination_iso3
            and s.snapshot_date = a.snapshot_date
    cross join quality as q
)

select
    destination_iso2,
    destination_iso3,
    destination_name,
    snapshot_date,
    reporting_issuer_count,
    reporting_issuers,
    normalized_ordinal_min,
    normalized_ordinal_median,
    normalized_ordinal_max,
    normalized_ordinal_range,
    latest_official_ingested_at,
    risk_direction,
    disagreement_direction,
    has_high_disagreement,
    changed_issuer_count,
    worsening_issuer_count,
    improving_issuer_count,
    matched_theme_count,
    matched_themes,
    context_alert_count,
    context_alert_types,
    has_warning_context_alert,
    gdelt_event_count_1d,
    gdelt_material_conflict_events_1d,
    gdelt_material_conflict_share_1d,
    gdelt_event_count_7d,
    gdelt_material_conflict_events_7d,
    gdelt_material_conflict_share_7d,
    gdelt_latest_event_date,
    gdelt_is_fresh,
    required_source_count,
    usable_required_source_count,
    all_required_sources_usable,
    gdelt_source_usable
from assembled
