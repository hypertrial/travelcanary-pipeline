select
    destination_iso3,
    snapshot_date
from {{ ref('country_risk_trends') }}
group by 1, 2
having count(*) > 1
