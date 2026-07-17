# Source coverage and caveats

## Official sources

| Source | Identifier | Native caveat |
| --- | --- | --- |
| US State Department | `us_state` | Published Level 1–4 from the JSON catalog, with its official RSS/XML feed as a structured fallback when the catalog is empty |
| Global Affairs Canada | `ca_gac` | Published advisory state 1–4 |
| UK FCDO | `uk_fcdo` | `alert_status` categories are mapped best-effort |
| Netherlands MFA | `nl_mfa` | Published colour categories |
| Japan MOFA | `jp_mofa` | Highest active regional risk level; all-zero counts remain null, not synthetic “safe” |

Official sources are required full catalogs. Static floors are 80% of committed healthy audit baselines; a batch is rejected below its floor or below 80% of its previous accepted count and warned below 90%. Canonical coverage must be at least 98%; normalization coverage among non-null native rows must be at least 99%, excluding documented exceptions.

Regional and compound identifiers remain observable raw records but are excluded from country marts when they cannot resolve to one ISO-3166-1 country. Examples include `PT-20`, Kosovo, and multi-territory UK pages.

## GDELT

TravelCanary uses the complete GDELT 1 daily Events export because it is a compact local-first source with stable daily files and the fields needed for country activity context. At least 95% of non-empty action-country codes must map to ISO3; coverage below 98% is visible as a warning.

The marts expose one- and seven-day event counts, material-conflict counts/shares, weighted average Goldstein scale, average tone, latest event date, freshness, root event type counts, and mention volume. These describe media-reported activity and are not an independent safety recommendation. GDELT 2 support is deferred.

Australia Smartraveller, Germany's Federal Foreign Office, and
travel-advisory.info were removed in 0.3.0. Australia failed the release gate
because no supported complete structured endpoint and compatible automated
reuse terms were documented; TravelCanary does not substitute HTML scraping.
Germany and the aggregator were removed from the public product scope.
Australia and Germany remain ordinary destinations in ISO mappings and in
other issuers' catalogs.

See the canonical [source licence and attribution
matrix](https://github.com/hypertrial/travelcanary-pipeline/blob/main/THIRD_PARTY_NOTICES.md)
for provenance, transformation, redistribution, and no-endorsement terms.
