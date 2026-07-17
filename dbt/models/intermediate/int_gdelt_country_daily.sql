with events as (
    select
        action_geo_country_code,
        event_date,
        quad_class,
        goldstein_scale,
        avg_tone,
        num_mentions
    from {{ ref('stg_gdelt_events') }}
),

fips as (
    select
        fips10_4,
        iso3
    from {{ ref('fips10_4_to_iso3') }}
)

select
    e.event_date,
    f.iso3 as destination_iso3,
    count(*) as event_count,
    sum(coalesce(e.num_mentions, 0)) as mention_count,
    sum(case when e.quad_class = 4 then 1 else 0 end) as material_conflict_events,
    sum(e.goldstein_scale) as goldstein_sum,
    count(e.goldstein_scale) as goldstein_count,
    avg(e.goldstein_scale) as avg_goldstein_scale,
    sum(e.avg_tone) as tone_sum,
    count(e.avg_tone) as tone_count,
    avg(e.avg_tone) as avg_tone
from events as e
inner join fips as f
    on e.action_geo_country_code = f.fips10_4
where e.event_date is not null
group by 1, 2
