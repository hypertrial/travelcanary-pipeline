select 1 as failure
where not exists (select 1 from {{ ref('int_gdelt_country_daily') }})
