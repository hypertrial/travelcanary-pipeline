with events as (
    select
        event_date,
        action_geo_country_code,
        quad_class,
        goldstein_scale,
        avg_tone,
        num_mentions,
        case
            when try_cast(event_root_code as integer) is not null
                then lpad(cast(try_cast(event_root_code as integer) as varchar), 2, '0')
            else event_root_code
        end as normalized_event_root_code
    from {{ ref('stg_gdelt_events') }}
),

fips as (
    select
        fips10_4,
        iso3
    from {{ ref('fips10_4_to_iso3') }}
),

root_codes as (
    select
        event_root_code,
        event_root_label
    from {{ ref('gdelt_event_root_codes') }}
)

select
    e.event_date,
    f.iso3 as destination_iso3,
    e.normalized_event_root_code as event_root_code,
    coalesce(r.event_root_label, 'Unknown') as event_root_label,
    count(*) as event_count,
    sum(coalesce(e.num_mentions, 0)) as mention_count,
    sum(case when e.quad_class = 4 then 1 else 0 end) as material_conflict_events,
    sum(case when e.quad_class = 4 then 1 else 0 end) / nullif(count(*), 0) as material_conflict_share,
    avg(e.goldstein_scale) as avg_goldstein_scale,
    avg(e.avg_tone) as avg_tone
from events as e
inner join fips as f
    on e.action_geo_country_code = f.fips10_4
left join root_codes as r
    on e.normalized_event_root_code = r.event_root_code
where
    e.event_date is not null
    and e.normalized_event_root_code is not null
group by 1, 2, 3, 4
