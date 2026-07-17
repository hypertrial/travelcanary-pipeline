with accepted as (
    select distinct source_run_id
    from {{ source('travelcanary_ops', 'source_sync_runs') }}
    where source = 'gdelt' and status = 'accepted'
)

select
    e.source_run_id,
    e.global_event_id,
    e.action_geo_country_code,
    e.event_root_code,
    e.quad_class,
    e.goldstein_scale,
    e.avg_tone,
    e.num_mentions,
    e.date_added,
    e.source_url,
    try_strptime(e.event_date, '%Y%m%d')::date as event_date,
    try_cast(e.ingested_at as timestamp) as ingested_at
from {{ source('gdelt_raw', 'events') }} as e
inner join accepted on e.source_run_id = accepted.source_run_id
