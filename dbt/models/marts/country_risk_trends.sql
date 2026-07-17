with thresholds as (
    select
        max(
            case
                when threshold_name = 'high_disagreement_range_min'
                    then cast(threshold_value as double)
            end
        ) as high_disagreement_range_min
    from {{ ref('insight_thresholds') }}
),

trends as (
    select
        destination_iso2,
        destination_iso3,
        destination_name,
        snapshot_date,
        reporting_issuer_count,
        normalized_ordinal_min,
        normalized_ordinal_median,
        normalized_ordinal_max,
        normalized_ordinal_range,
        previous_snapshot_date,
        previous_normalized_ordinal_median,
        normalized_ordinal_median_delta,
        risk_direction,
        previous_normalized_ordinal_range,
        normalized_ordinal_range_delta,
        disagreement_direction,
        latest_official_ingested_at
    from {{ ref('int_country_risk_trend_baseline') }}
),

classified as (
    select
        t.destination_iso2,
        t.destination_iso3,
        t.destination_name,
        t.snapshot_date,
        t.reporting_issuer_count,
        t.normalized_ordinal_min,
        t.normalized_ordinal_median,
        t.normalized_ordinal_max,
        t.normalized_ordinal_range,
        t.previous_snapshot_date,
        t.previous_normalized_ordinal_median,
        t.normalized_ordinal_median_delta,
        t.risk_direction,
        t.previous_normalized_ordinal_range,
        t.normalized_ordinal_range_delta,
        t.disagreement_direction,
        t.latest_official_ingested_at,
        coalesce(
            t.normalized_ordinal_range >= h.high_disagreement_range_min,
            false
        ) as has_high_disagreement
    from trends as t
    cross join thresholds as h
)

select
    destination_iso2,
    destination_iso3,
    destination_name,
    snapshot_date,
    reporting_issuer_count,
    normalized_ordinal_min,
    normalized_ordinal_median,
    normalized_ordinal_max,
    normalized_ordinal_range,
    previous_snapshot_date,
    previous_normalized_ordinal_median,
    normalized_ordinal_median_delta,
    risk_direction,
    previous_normalized_ordinal_range,
    normalized_ordinal_range_delta,
    disagreement_direction,
    has_high_disagreement,
    latest_official_ingested_at
from classified
