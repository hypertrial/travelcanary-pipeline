select
    issuing_government as source,
    destination_native_id,
    count(*) as unresolved_rows
from {{ ref('int_advisory_level_normalized') }}
where destination_iso3 is null
group by 1, 2
