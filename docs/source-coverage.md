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

## Candidate issuers evaluated in 0.5.0

TravelCanary evaluated three additional official issuers against the same
gate that removed Australia in 0.3.0: a documented, supported, complete
structured endpoint (JSON, XML, or API — not HTML scraping) plus compatible
automated-reuse terms. None were adopted in 0.5.0.

| Candidate | Endpoint finding | Format / completeness | Reuse terms | Verdict |
| --- | --- | --- | --- | --- |
| New Zealand SafeTravel (MFAT) | Destination catalog is HTML (`https://www.safetravel.govt.nz/destinations`). Previously documented news/warnings RSS paths now redirect or 404 after the site redesign; no complete machine-readable advisory catalog was found (accessed 2026-07-18). | HTML destination pages with four advice levels; no supported complete JSON/XML/API catalog | Crown copyright licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) for text content; logos and imagery excluded ([SafeTravel copyright](https://www.safetravel.govt.nz/copyright), accessed 2026-07-18) | Fail — reuse terms are compatible, but no complete structured endpoint |
| Ireland DFA Travel Advice | Public site is HTML (`https://www.ireland.ie/en/dfa/overseas-travel/advice/`). No documented complete structured catalog; community scrapers use browser automation against destination pages (accessed 2026-07-18). | Per-destination HTML pages | Irish Public Sector Open Data uses [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) for datasets published as open data; travel-advice HTML is not published as an open structured dataset, and TravelWise branding requires prior permission (accessed 2026-07-18) | Fail — no supported complete structured endpoint; automated reuse of the HTML catalog is not documented |
| France Conseils aux Voyageurs (MEAE) | Public site is HTML (`https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/`). No production advisory API; `data.gouv.fr` hosts a usage-study page rather than a complete advisory catalog feed; `/conseils-aux-voyageurs/rss` returns 404 (accessed 2026-07-18). | HTML destination selector (~191 destinations per MEAE communications) | French public-site content remains under MEAE site terms; no open-data licence covering a complete machine-readable advisory catalog was identified (accessed 2026-07-18) | Fail — no supported complete structured endpoint with confirmed automated-reuse terms |

Passing candidates would be noted here as adoption-ready for a later release.
Nothing from this table is ingested in 0.5.0.

See the canonical [source licence and attribution
matrix](https://github.com/hypertrial/travelcanary-pipeline/blob/main/THIRD_PARTY_NOTICES.md)
for provenance, transformation, redistribution, and no-endorsement terms.
