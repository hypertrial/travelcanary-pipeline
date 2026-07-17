select
    destination_iso3,
    issuing_government,
    snapshot_date
from {{ ref('country_advisory_changes') }}
group by 1, 2, 3
having count(*) > 1
