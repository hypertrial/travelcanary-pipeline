select
    e.action_geo_country_code,
    count(*) as unmapped_rows
from {{ ref('stg_gdelt_events') }} as e
left join {{ ref('fips10_4_to_iso3') }} as f
    on e.action_geo_country_code = f.fips10_4
where e.action_geo_country_code is not null and f.iso3 is null
group by 1
