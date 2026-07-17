select
    source_run_id,
    source,
    status,
    finished_at,
    fetched_rows,
    previous_accepted_rows,
    relative_ratio,
    message
from {{ source('travelcanary_ops', 'source_sync_runs') }}
where
    status in ('rejected', 'error', 'unavailable')
    or (relative_ratio is not null and relative_ratio < 0.90)
