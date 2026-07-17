select
    event_date,
    destination_iso3,
    event_root_code,
    event_root_label,
    event_count,
    mention_count,
    material_conflict_events,
    material_conflict_share,
    avg_goldstein_scale,
    avg_tone
from {{ ref('int_gdelt_country_event_types_daily') }}
