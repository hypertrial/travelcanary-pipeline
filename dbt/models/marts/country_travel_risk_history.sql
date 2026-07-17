{{ config(materialized='incremental', unique_key=['destination_iso3', 'issuing_government', 'snapshot_date']) }}

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
    normalization_status,
    summary_text,
    source_url,
    source_published_at,
    source_ingested_at,
    current_timestamp as snapshot_recorded_at
from {{ ref('country_travel_risk') }}

{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), date '1900-01-01') from {{ this }}
)
{% endif %}
