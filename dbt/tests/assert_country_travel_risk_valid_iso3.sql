select r.destination_iso3
from {{ ref('country_travel_risk') }} as r
left join {{ ref('iso_countries') }} as c
    on r.destination_iso3 = c.iso3
where c.iso3 is null
