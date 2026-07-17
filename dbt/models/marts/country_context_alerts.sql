with thresholds as (
    select
        max(
            case
                when threshold_name = 'low_official_median_max'
                    then cast(threshold_value as double)
            end
        ) as low_official_median_max,
        max(
            case
                when threshold_name = 'high_official_median_min'
                    then cast(threshold_value as double)
            end
        ) as high_official_median_min,
        max(
            case
                when threshold_name = 'high_gdelt_conflict_events_7d_min'
                    then cast(threshold_value as double)
            end
        ) as high_gdelt_conflict_events_7d_min,
        max(
            case
                when threshold_name = 'high_gdelt_conflict_share_7d_min'
                    then cast(threshold_value as double)
            end
        ) as high_gdelt_conflict_share_7d_min,
        max(
            case
                when threshold_name = 'high_gdelt_mentions_7d_min'
                    then cast(threshold_value as double)
            end
        ) as high_gdelt_mentions_7d_min,
        max(
            case
                when threshold_name = 'quiet_gdelt_event_count_7d_max'
                    then cast(threshold_value as double)
            end
        ) as quiet_gdelt_event_count_7d_max
    from {{ ref('insight_thresholds') }}
),

gdelt_rolling as (
    select
        event_date,
        destination_iso3,
        mention_count_7d
    from {{ ref('int_gdelt_country_rolling_7d') }}
    qualify row_number() over (
        partition by destination_iso3 order by event_date desc
    ) = 1
),

base as (
    select
        s.destination_iso2,
        s.destination_iso3,
        s.destination_name,
        s.snapshot_date,
        s.normalized_ordinal_median,
        s.normalized_ordinal_max,
        s.normalized_ordinal_range,
        s.gdelt_event_count_7d,
        s.gdelt_material_conflict_events_7d,
        s.gdelt_material_conflict_share_7d,
        s.gdelt_latest_event_date,
        s.gdelt_is_fresh,
        coalesce(r.mention_count_7d, 0) as gdelt_mention_count_7d
    from {{ ref('country_risk_signals') }} as s
    left join gdelt_rolling as r
        on s.destination_iso3 = r.destination_iso3
)

select
    b.destination_iso2,
    b.destination_iso3,
    b.destination_name,
    b.snapshot_date,
    'official_low_gdelt_high' as alert_type,
    'warning' as severity,
    b.normalized_ordinal_median as official_median_ordinal,
    b.normalized_ordinal_max as official_max_ordinal,
    b.normalized_ordinal_range as official_disagreement_range,
    b.gdelt_latest_event_date,
    b.gdelt_event_count_7d,
    b.gdelt_material_conflict_events_7d,
    b.gdelt_material_conflict_share_7d,
    b.gdelt_mention_count_7d,
    'Official median is low while GDELT conflict or mention volume is high' as reason
from base as b
cross join thresholds as t
where
    b.gdelt_is_fresh
    and b.normalized_ordinal_median <= t.low_official_median_max
    and (
        coalesce(b.gdelt_material_conflict_events_7d, 0) >= t.high_gdelt_conflict_events_7d_min
        or coalesce(b.gdelt_material_conflict_share_7d, 0) >= t.high_gdelt_conflict_share_7d_min
        or coalesce(b.gdelt_mention_count_7d, 0) >= t.high_gdelt_mentions_7d_min
    )

union all

select
    b.destination_iso2,
    b.destination_iso3,
    b.destination_name,
    b.snapshot_date,
    'official_high_gdelt_quiet' as alert_type,
    'info' as severity,
    b.normalized_ordinal_median as official_median_ordinal,
    b.normalized_ordinal_max as official_max_ordinal,
    b.normalized_ordinal_range as official_disagreement_range,
    b.gdelt_latest_event_date,
    b.gdelt_event_count_7d,
    b.gdelt_material_conflict_events_7d,
    b.gdelt_material_conflict_share_7d,
    b.gdelt_mention_count_7d,
    'Official median is high while recent GDELT event volume is quiet' as reason
from base as b
cross join thresholds as t
where
    b.gdelt_is_fresh
    and b.normalized_ordinal_median >= t.high_official_median_min
    and coalesce(b.gdelt_event_count_7d, 0) <= t.quiet_gdelt_event_count_7d_max
