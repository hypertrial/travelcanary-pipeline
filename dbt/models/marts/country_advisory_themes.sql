select
    destination_iso2,
    destination_iso3,
    destination_name,
    issuing_government,
    snapshot_date,
    theme,
    matched_keyword_count,
    matched_terms
from {{ ref('int_advisory_theme_matches') }}
