select
    destination_iso3,
    snapshot_date,
    alert_type
from {{ ref('country_context_alerts') }}
group by 1, 2, 3
having count(*) > 1
