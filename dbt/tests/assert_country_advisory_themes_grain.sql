select
    destination_iso3,
    issuing_government,
    snapshot_date,
    theme
from {{ ref('country_advisory_themes') }}
group by 1, 2, 3, 4
having count(*) > 1
