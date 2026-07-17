with daily as (
    select
        event_date,
        destination_iso3,
        event_count,
        mention_count,
        material_conflict_events,
        goldstein_sum,
        goldstein_count,
        tone_sum,
        tone_count
    from {{ ref('int_gdelt_country_daily') }}
)

select
    anchor.event_date,
    anchor.destination_iso3,
    sum(rollup_day.event_count) as event_count_7d,
    sum(rollup_day.mention_count) as mention_count_7d,
    sum(rollup_day.material_conflict_events) as material_conflict_events_7d,
    sum(rollup_day.material_conflict_events) / nullif(sum(rollup_day.event_count), 0) as material_conflict_share_7d,
    sum(rollup_day.goldstein_sum) / nullif(sum(rollup_day.goldstein_count), 0) as avg_goldstein_scale_7d,
    sum(rollup_day.tone_sum) / nullif(sum(rollup_day.tone_count), 0) as avg_tone_7d
from daily as anchor
inner join daily as rollup_day
    on
        anchor.destination_iso3 = rollup_day.destination_iso3
        and rollup_day.event_date between anchor.event_date - interval 6 day and anchor.event_date
group by 1, 2
