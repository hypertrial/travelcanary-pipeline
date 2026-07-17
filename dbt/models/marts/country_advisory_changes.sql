with history as (
    select
        destination_iso2,
        destination_iso3,
        destination_name,
        issuing_government,
        snapshot_date,
        native_level,
        native_level_label,
        normalized_ordinal,
        normalized_level_label,
        source_url,
        source_published_at,
        source_ingested_at,
        lag(snapshot_date) over (
            partition by destination_iso3, issuing_government order by snapshot_date
        ) as previous_snapshot_date,
        lag(normalized_ordinal) over (
            partition by destination_iso3, issuing_government order by snapshot_date
        ) as previous_normalized_ordinal
    from {{ ref('country_travel_risk_history') }}
),

classified as (
    select
        destination_iso2,
        destination_iso3,
        destination_name,
        issuing_government,
        snapshot_date,
        native_level,
        native_level_label,
        normalized_ordinal,
        normalized_level_label,
        previous_snapshot_date,
        previous_normalized_ordinal,
        source_url,
        source_published_at,
        source_ingested_at,
        normalized_ordinal - previous_normalized_ordinal as normalized_ordinal_delta,
        case
            when previous_snapshot_date is null then 'new'
            when normalized_ordinal is null or previous_normalized_ordinal is null then 'unknown'
            when normalized_ordinal - previous_normalized_ordinal > 0 then 'worsening'
            when normalized_ordinal - previous_normalized_ordinal < 0 then 'improving'
            else 'unchanged'
        end as advisory_direction,
        coalesce(
            normalized_ordinal - previous_normalized_ordinal != 0,
            false
        ) as is_changed
    from history
    where destination_iso3 is not null
)

select
    destination_iso2,
    destination_iso3,
    destination_name,
    issuing_government,
    snapshot_date,
    native_level,
    native_level_label,
    normalized_ordinal,
    normalized_level_label,
    previous_snapshot_date,
    previous_normalized_ordinal,
    normalized_ordinal_delta,
    advisory_direction,
    is_changed,
    source_url,
    source_published_at,
    source_ingested_at
from classified
