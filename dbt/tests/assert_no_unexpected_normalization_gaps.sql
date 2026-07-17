select gaps.*
from {{ ref('advisory_normalization_gaps') }} as gaps
left join {{ ref('advisory_normalization_exceptions') }} as exceptions
    on
        gaps.source = exceptions.source
        and gaps.native_level = exceptions.native_value
where exceptions.source is null
