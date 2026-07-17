{{ config(materialized='table') }}

with advisories as (
    select * from {{ ref('stg_us_state_advisories') }}
    union all
    select * from {{ ref('stg_ca_gac_advisories') }}
    union all
    select * from {{ ref('stg_uk_fcdo_advisories') }}
    union all
    select * from {{ ref('stg_nl_mfa_advisories') }}
    union all
    select * from {{ ref('stg_jp_mofa_advisories') }}
),

accepted as (
    select
        source,
        source_run_id
    from {{ source('travelcanary_ops', 'source_sync_runs') }}
    where status = 'accepted'
    qualify row_number() over (
        partition by source
        order by finished_at desc
    ) = 1
)

select a.*
from advisories as a
inner join accepted as r
    on a.source = r.source and a.source_run_id = r.source_run_id
