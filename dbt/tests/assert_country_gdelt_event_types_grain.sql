select
    destination_iso3,
    event_date,
    event_root_code
from {{ ref('country_gdelt_event_types') }}
group by 1, 2, 3
having count(*) > 1
