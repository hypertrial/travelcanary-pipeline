with thresholds as (
    select
        max(
            case
                when threshold_name = 'significant_median_delta_min'
                    then cast(threshold_value as double)
            end
        ) as significant_median_delta_min
    from {{ ref('insight_thresholds') }}
),

daily as (
    select
        destination_iso2,
        destination_iso3,
        destination_name,
        snapshot_date,
        count(*) as reporting_issuer_count,
        min(normalized_ordinal) as normalized_ordinal_min,
        median(normalized_ordinal) as normalized_ordinal_median,
        max(normalized_ordinal) as normalized_ordinal_max,
        max(normalized_ordinal) - min(normalized_ordinal) as normalized_ordinal_range,
        max(source_ingested_at) as latest_official_ingested_at
    from {{ ref('country_travel_risk_history') }}
    where destination_iso3 is not null
    group by 1, 2, 3, 4
),

with_previous as (
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
        latest_official_ingested_at,
        lag(snapshot_date) over (
            partition by destination_iso3 order by snapshot_date
        ) as previous_snapshot_date,
        lag(normalized_ordinal_median) over (
            partition by destination_iso3 order by snapshot_date
        ) as previous_normalized_ordinal_median,
        lag(normalized_ordinal_range) over (
            partition by destination_iso3 order by snapshot_date
        ) as previous_normalized_ordinal_range
    from daily
),

with_deltas as (
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
        latest_official_ingested_at,
        previous_snapshot_date,
        previous_normalized_ordinal_median,
        previous_normalized_ordinal_range,
        normalized_ordinal_median - previous_normalized_ordinal_median as normalized_ordinal_median_delta,
        normalized_ordinal_range - previous_normalized_ordinal_range as normalized_ordinal_range_delta
    from with_previous
)

select
    w.destination_iso2,
    w.destination_iso3,
    w.destination_name,
    w.snapshot_date,
    w.reporting_issuer_count,
    w.normalized_ordinal_min,
    w.normalized_ordinal_median,
    w.normalized_ordinal_max,
    w.normalized_ordinal_range,
    w.previous_snapshot_date,
    w.previous_normalized_ordinal_median,
    w.normalized_ordinal_median_delta,
    w.previous_normalized_ordinal_range,
    w.normalized_ordinal_range_delta,
    w.latest_official_ingested_at,
    case
        when w.previous_snapshot_date is null then 'new'
        when w.normalized_ordinal_median is null or w.previous_normalized_ordinal_median is null then 'unknown'
        when w.normalized_ordinal_median_delta >= t.significant_median_delta_min then 'worsening'
        when w.normalized_ordinal_median_delta <= -t.significant_median_delta_min then 'improving'
        else 'unchanged'
    end as risk_direction,
    case
        when w.previous_snapshot_date is null then 'new'
        when w.normalized_ordinal_range is null or w.previous_normalized_ordinal_range is null then 'unknown'
        when w.normalized_ordinal_range_delta > 0 then 'increasing'
        when w.normalized_ordinal_range_delta < 0 then 'decreasing'
        else 'unchanged'
    end as disagreement_direction
from with_deltas as w
cross join thresholds as t
