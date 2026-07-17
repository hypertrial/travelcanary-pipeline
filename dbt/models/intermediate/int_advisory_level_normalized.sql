with advisories as (
    select
        source_run_id,
        advisory_id,
        source,
        destination_native_id,
        destination_iso2,
        destination_iso3,
        destination_name,
        native_level,
        native_level_label,
        summary_text,
        source_url,
        published_at,
        ingested_at,
        upper(destination_iso2) as destination_iso2_key,
        upper(destination_iso3) as destination_iso3_key
    from {{ ref('stg_all_advisories') }}
),

crosswalk as (
    select
        source,
        destination_native_id,
        upper(destination_iso3) as destination_iso3_key
    from {{ ref('country_code_crosswalk') }}
),

countries as (
    select
        country_name,
        upper(iso2) as iso2,
        upper(iso3) as iso3
    from {{ ref('iso_countries') }}
),

normalization as (
    select
        source,
        native_value,
        normalized_ordinal,
        normalized_label
    from {{ ref('advisory_level_normalization') }}
),

resolved as (
    select
        a.source_run_id,
        a.advisory_id,
        a.source,
        a.destination_native_id,
        a.destination_iso2,
        a.destination_iso3,
        a.destination_name,
        a.native_level,
        a.native_level_label,
        a.summary_text,
        a.source_url,
        a.published_at,
        a.ingested_at,
        coalesce(cw_country.iso2, by_iso3.iso2, by_iso2.iso2) as resolved_iso2,
        coalesce(cw_country.iso3, by_iso3.iso3, by_iso2.iso3) as resolved_iso3,
        coalesce(cw_country.country_name, by_iso3.country_name, by_iso2.country_name) as canonical_name
    from advisories as a
    left join crosswalk as cw
        on
            a.source = cw.source
            and a.destination_native_id = cw.destination_native_id
    left join countries as cw_country
        on cw.destination_iso3_key = cw_country.iso3
    left join countries as by_iso3
        on a.destination_iso3_key = by_iso3.iso3
    left join countries as by_iso2
        on a.destination_iso2_key = by_iso2.iso2
)

select
    r.source_run_id,
    r.advisory_id,
    r.source as issuing_government,
    r.resolved_iso2 as destination_iso2,
    r.resolved_iso3 as destination_iso3,
    r.canonical_name as destination_name,
    r.destination_native_id,
    r.native_level,
    r.native_level_label,
    n.normalized_ordinal,
    n.normalized_label as normalized_level_label,
    r.summary_text,
    r.source_url,
    cast(current_date as date) as snapshot_date,
    case
        when r.native_level is null then 'no_native_level'
        when n.native_value is null then 'unmapped'
        else 'normalized'
    end as normalization_status,
    try_cast(r.published_at as timestamp) as source_published_at,
    try_cast(r.ingested_at as timestamp) as source_ingested_at
from resolved as r
left join normalization as n
    on
        r.source = n.source
        and coalesce(r.native_level, '') = n.native_value
