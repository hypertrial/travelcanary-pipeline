select
    source_run_id,
    advisory_id,
    source,
    destination_native_id,
    destination_iso2,
    destination_iso3,
    destination_name,
    native_level,
    native_level_label,
    summary_text,
    source_url,
    published_at,
    ingested_at
from {{ source('nl_mfa_raw', 'advisories') }}
