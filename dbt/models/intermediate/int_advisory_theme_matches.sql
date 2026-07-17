with advisories as (
    select
        destination_iso2,
        destination_iso3,
        destination_name,
        issuing_government,
        snapshot_date,
        lower(
            coalesce(summary_text, '') || ' '
            || coalesce(native_level_label, '') || ' '
            || coalesce(normalized_level_label, '')
        ) as theme_text
    from {{ ref('country_travel_risk_history') }}
    where destination_iso3 is not null
),

keywords as (
    select
        theme,
        lower(keyword) as keyword
    from {{ ref('advisory_theme_keywords') }}
)

select
    a.destination_iso2,
    a.destination_iso3,
    a.destination_name,
    a.issuing_government,
    a.snapshot_date,
    k.theme,
    count(*) as matched_keyword_count,
    string_agg(k.keyword, ', ') as matched_terms
from advisories as a
inner join keywords as k
    on contains(a.theme_text, k.keyword)
group by 1, 2, 3, 4, 5, 6
