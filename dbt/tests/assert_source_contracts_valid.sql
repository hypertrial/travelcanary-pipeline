with expected_sources (source, role) as (
    values
    ('us_state', 'required'),
    ('ca_gac', 'required'),
    ('uk_fcdo', 'required'),
    ('nl_mfa', 'required'),
    ('jp_mofa', 'required'),
    ('gdelt', 'required')
),

invalid_values as (
    select source
    from {{ ref('source_contracts') }}
    where
        minimum_rows <= 0
        or warn_after_hours <= 0
        or error_after_hours <= 0
        or warn_after_hours >= error_after_hours
        or warn_drop_ratio not between 0 and 1
        or reject_drop_ratio not between 0 and 1
        or reject_drop_ratio > warn_drop_ratio
        or minimum_canonical_ratio not between 0 and 1
        or warn_canonical_ratio not between 0 and 1
        or minimum_canonical_ratio > warn_canonical_ratio
        or minimum_normalization_ratio not between 0 and 1
),

unexpected_sources as (
    select
        source,
        role
    from {{ ref('source_contracts') }}
    except
    select
        source,
        role
    from expected_sources
),

missing_sources as (
    select
        source,
        role
    from expected_sources
    except
    select
        source,
        role
    from {{ ref('source_contracts') }}
)

select source from invalid_values
union all
select source from unexpected_sources
union all
select source from missing_sources
