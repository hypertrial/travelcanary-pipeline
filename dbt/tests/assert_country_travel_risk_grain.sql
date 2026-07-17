select
    destination_iso3,
    issuing_government,
    snapshot_date,
    count(*) as row_count
from {{ ref('country_travel_risk') }}
group by 1, 2, 3
having count(*) > 1
