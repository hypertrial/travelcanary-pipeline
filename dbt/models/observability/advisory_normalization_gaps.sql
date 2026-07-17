select
    issuing_government as source,
    native_level,
    count(*) as unmapped_rows
from {{ ref('int_advisory_level_normalized') }}
where normalization_status = 'unmapped'
group by 1, 2
