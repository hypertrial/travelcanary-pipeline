with health as (
    select
        source,
        role,
        health_status,
        latest_status,
        latest_finished_at,
        fetched_rows,
        previous_accepted_rows,
        relative_ratio,
        canonical_ratio,
        normalization_ratio,
        message
    from {{ ref('source_health') }}
),

contracts as (
    select
        source,
        warn_drop_ratio,
        reject_drop_ratio,
        warn_after_hours,
        error_after_hours,
        minimum_canonical_ratio,
        warn_canonical_ratio,
        minimum_normalization_ratio
    from {{ ref('source_contracts') }}
),

quality as (
    select
        h.source,
        h.role,
        h.health_status,
        h.latest_status,
        h.latest_finished_at,
        h.fetched_rows,
        h.previous_accepted_rows,
        h.relative_ratio,
        h.canonical_ratio,
        h.normalization_ratio,
        h.message,
        case
            when h.latest_finished_at is null then null
            else date_diff('hour', h.latest_finished_at, current_timestamp)
        end as hours_since_latest_run,
        h.health_status = 'healthy' as is_healthy,
        h.health_status in ('healthy', 'warning') as is_usable,
        case
            when h.latest_finished_at is null then 'unavailable'
            when date_diff('hour', h.latest_finished_at, current_timestamp) >= c.error_after_hours then 'stale'
            when date_diff('hour', h.latest_finished_at, current_timestamp) >= c.warn_after_hours then 'warning'
            else 'fresh'
        end as freshness_status,
        case
            when h.relative_ratio is null then 'unknown'
            when h.relative_ratio < c.reject_drop_ratio then 'rejected'
            when h.relative_ratio < c.warn_drop_ratio then 'warning'
            else 'healthy'
        end as volume_status,
        case
            when h.canonical_ratio is null then 'unknown'
            when h.canonical_ratio < c.minimum_canonical_ratio then 'rejected'
            when h.canonical_ratio < c.warn_canonical_ratio then 'warning'
            else 'healthy'
        end as canonical_status,
        case
            when c.minimum_normalization_ratio = 0 or h.normalization_ratio is null then 'not_applicable'
            when h.normalization_ratio < c.minimum_normalization_ratio then 'rejected'
            else 'healthy'
        end as normalization_status
    from health as h
    inner join contracts as c
        on h.source = c.source
)

select
    source,
    role,
    health_status,
    latest_status,
    latest_finished_at,
    hours_since_latest_run,
    fetched_rows,
    previous_accepted_rows,
    relative_ratio,
    canonical_ratio,
    normalization_ratio,
    message,
    is_healthy,
    is_usable,
    freshness_status,
    volume_status,
    canonical_status,
    normalization_status
from quality
