with official_rows as (
    select distinct
        destination_iso2,
        destination_iso3,
        destination_name,
        issuing_government,
        snapshot_date,
        normalized_ordinal,
        source_ingested_at
    from {{ ref('country_travel_risk') }}
),

official as (
    select
        destination_iso2,
        destination_iso3,
        destination_name,
        snapshot_date,
        count(issuing_government) as reporting_issuer_count,
        min(normalized_ordinal) as normalized_ordinal_min,
        median(normalized_ordinal) as normalized_ordinal_median,
        max(normalized_ordinal) as normalized_ordinal_max,
        max(normalized_ordinal) - min(normalized_ordinal) as normalized_ordinal_range,
        max(source_ingested_at) as latest_official_ingested_at
    from official_rows
    group by 1, 2, 3, 4
),

gdelt_daily as (
    select
        event_date,
        destination_iso3,
        event_count,
        material_conflict_events,
        avg_goldstein_scale,
        avg_tone
    from {{ ref('int_gdelt_country_daily') }}
    qualify row_number() over (
        partition by destination_iso3 order by event_date desc
    ) = 1
),

gdelt_rolling as (
    select
        event_date,
        destination_iso3,
        event_count_7d,
        material_conflict_events_7d,
        material_conflict_share_7d,
        avg_goldstein_scale_7d,
        avg_tone_7d
    from {{ ref('int_gdelt_country_rolling_7d') }}
    qualify row_number() over (
        partition by destination_iso3 order by event_date desc
    ) = 1
)

select
    o.destination_iso2,
    o.destination_iso3,
    o.destination_name,
    o.snapshot_date,
    o.reporting_issuer_count,
    o.normalized_ordinal_min,
    o.normalized_ordinal_median,
    o.normalized_ordinal_max,
    o.normalized_ordinal_range,
    o.latest_official_ingested_at,
    d.event_count as gdelt_event_count_1d,
    d.material_conflict_events as gdelt_material_conflict_events_1d,
    d.avg_goldstein_scale as gdelt_avg_goldstein_scale_1d,
    d.avg_tone as gdelt_avg_tone_1d,
    r.event_count_7d as gdelt_event_count_7d,
    r.material_conflict_events_7d as gdelt_material_conflict_events_7d,
    r.material_conflict_share_7d as gdelt_material_conflict_share_7d,
    r.avg_goldstein_scale_7d as gdelt_avg_goldstein_scale_7d,
    r.avg_tone_7d as gdelt_avg_tone_7d,
    d.event_date as gdelt_latest_event_date,
    d.material_conflict_events / nullif(d.event_count, 0) as gdelt_material_conflict_share_1d,
    coalesce(d.event_date >= current_date - interval 3 day, false) as gdelt_is_fresh
from official as o
left join gdelt_daily as d on o.destination_iso3 = d.destination_iso3
left join gdelt_rolling as r on o.destination_iso3 = r.destination_iso3
