with contracts as (
    select * from {{ ref('source_contracts') }}
),

latest as (
    select *
    from {{ source('travelcanary_ops', 'source_sync_runs') }}
    qualify row_number() over (
        partition by source order by finished_at desc
    ) = 1
)

select
    c.source,
    c.role,
    l.status as latest_status,
    l.finished_at as latest_finished_at,
    l.fetched_rows,
    l.previous_accepted_rows,
    l.relative_ratio,
    l.canonical_ratio,
    l.normalization_ratio,
    l.message,
    case
        when l.source is null then 'unavailable'
        when l.status != 'accepted' then l.status
        when date_diff('hour', l.finished_at, current_timestamp) >= c.error_after_hours then 'stale'
        when date_diff('hour', l.finished_at, current_timestamp) >= c.warn_after_hours then 'warning'
        when l.relative_ratio is not null and l.relative_ratio < c.warn_drop_ratio then 'warning'
        when l.canonical_ratio is not null and l.canonical_ratio < c.warn_canonical_ratio then 'warning'
        else 'healthy'
    end as health_status
from contracts as c
left join latest as l on c.source = l.source
