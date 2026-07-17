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
    source_ingested_at
from {{ ref('int_advisory_level_normalized') }}
where destination_iso3 is not null
