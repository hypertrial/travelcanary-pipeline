# Third-party and source notices

Last reviewed: 2026-07-17.

The MIT licence in this repository covers Hypertrial's original software only.
It does not relicense government content, GDELT data, reference mappings, or
generated outputs. No downloaded advisory catalog, GDELT archive, or generated
warehouse is distributed in this repository. Test fixtures are minimal
synthetic records and are not source-data extracts.

TravelCanary and Hypertrial are not affiliated with or endorsed by any source
provider or government. Names and links identify data provenance only.

## Runtime source matrix

| Identifier | Content and endpoint | Governing notice | Required attribution and treatment |
| --- | --- | --- | --- |
| `ca_gac` | Global Affairs Canada travel-advisory index at `https://data.international.gc.ca/travel-voyage/index-alpha-eng.json` | [Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada), accessed 2026-07-17 | “Contains information licensed under the Open Government Licence – Canada.” TravelCanary parses, resolves country codes, normalizes the published state, and aggregates the result. Preserve the licence link, identify Global Affairs Canada as source, and do not imply official status or endorsement. |
| `uk_fcdo` | GOV.UK Content API foreign-travel-advice catalog | [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/), accessed 2026-07-17 | “Contains public sector information licensed under the Open Government Licence v3.0.” TravelCanary discovers pages, extracts alert categories, resolves countries, and applies a documented best-effort normalization. Preserve provider attribution and the licence link; exclude logos and third-party material; do not imply endorsement. |
| `nl_mfa` | NetherlandsWorldwide travel-advice open-data feed | [NederlandWereldwijd website and open-data terms](https://www.nederlandwereldwijd.nl/over-de-website), accessed 2026-07-17 | Website content is offered under CC0 except identified exceptions; photographs are excluded. TravelCanary uses the structured travel-advice feed, extracts colour categories, resolves countries, and aggregates results. Credit “NederlandWereldwijd, Ministry of Foreign Affairs of the Netherlands,” link the source, preserve any item-specific exception, and do not imply endorsement. |
| `jp_mofa` | Japan MOFA public-data country XML under `https://www.ezairyu.mofa.go.jp/opendata/country/` | [MOFA Legal Notices / Public Data License 1.0](https://www.mofa.go.jp/about/legalmatters.html), accessed 2026-07-17 | Source: Ministry of Foreign Affairs of Japan website. URL: `https://www.ezairyu.mofa.go.jp/opendata/country/`. Accessed 2026-07-17. TravelCanary creates edited content by selecting the highest active regional risk level, mapping MOFA country codes, normalizing values, and aggregating results. Identify it as edited by Hypertrial and never present it as created by MOFA. Check each item for third-party rights and excluded marks. |
| `us_state` | U.S. Department of State travel-advisory JSON catalog, with the official RSS/XML feed as fallback | [Travel.State.Gov copyright notice](https://travel.state.gov/content/travel/en/copyright-disclaimer.html), accessed 2026-07-17 | Unless a copyright is indicated, Consular Affairs information is public domain; attribution to the Bureau of Consular Affairs, U.S. Department of State is appreciated. TravelCanary parses, resolves countries, normalizes levels, and aggregates results. Do not redistribute separately copyrighted photos, graphics, or third-party material, and do not imply endorsement. |
| `gdelt` | GDELT 1 daily Events ZIP from the HTTPS Google Storage endpoint | [GDELT terms of use](https://www.gdeltproject.org/about.html), accessed 2026-07-17 | Any use or redistribution must cite the GDELT Project and link to `https://www.gdeltproject.org/`. TravelCanary selects the documented event fields, maps FIPS 10-4 action geography to ISO3, aggregates daily and seven-day counts, and derives contextual indicators. GDELT is media-derived context, not an official travel advisory. |

The source pages and terms can change. Recheck them before redistributing
source or derived data. A source-specific notice or exception accompanying a
record takes priority over this summary.

## Operational reference mappings

These files are necessary lookup or transformation rules, not downloaded live
payloads:

| Files | Provenance and transformation | Reuse treatment |
| --- | --- | --- |
| `dbt/seeds/iso_countries.csv` | ISO 3166-1 alpha-2/alpha-3 identifiers and English short names assembled from the `pycountry`/Debian `iso-codes` data used by the application, then manually reviewed for pipeline identity resolution. | Codes and names are not claimed as Hypertrial-authored data. Preserve provenance, the `pycountry` dependency notice, and any applicable ISO/`iso-codes` terms when redistributing the table. ISO does not endorse TravelCanary. |
| `src/travelcanary_pipeline/ingestion/jp_mofa/country_codes.csv` | MOFA numeric identifiers observed in the official public-data catalog, cross-referenced to ISO3 and given convenience names. | Governed by the MOFA Public Data License notice above; identify the table as an edited crosswalk and retain source URL/access date. |
| `src/travelcanary_pipeline/ingestion/uk_fcdo/country_slugs.csv` | GOV.UK foreign-travel-advice slugs discovered from the Content API, cross-referenced to ISO2/ISO3. | Governed by OGL v3.0; retain the OGL attribution and no-endorsement language. |
| `dbt/seeds/fips10_4_to_iso3.csv` | GDELT action-geography FIPS 10-4 codes cross-referenced to ISO3 for country aggregation. | Retain GDELT citation/link when the mapping is distributed with GDELT-derived data; do not imply that GDELT or any government endorses the crosswalk. |
| `dbt/seeds/country_code_crosswalk.csv` | Minimal source-native identifiers cross-referenced to ISO after manual review. | Each row retains the terms of its named source above. |
| `dbt/seeds/advisory_level_normalization.csv`, `dbt/seeds/advisory_normalization_exceptions.csv` | Hypertrial-authored, documented best-effort interpretations of source-native advisory labels. | MIT covers Hypertrial's mapping logic, not the underlying source labels. Modified mappings must remain clearly identified as interpretations, not official equivalence. |
| `dbt/seeds/gdelt_event_root_codes.csv` | CAMEO/GDELT root-event labels used to summarize GDELT data. | Retain the GDELT citation/link and review any separate CAMEO terms before standalone redistribution. |

Other thresholds and synthetic demo records in `dbt/seeds/` and
`tests/fixtures/` are Hypertrial-authored and covered by MIT unless a file
states otherwise.

## Dependencies

Runtime and development dependencies retain their own licences. The lockfile
is the authoritative version inventory; release evidence must include a
machine-generated dependency-licence report and review of any unknown or
non-permissive result. No dependency licence is replaced by this repository's
MIT licence.

## Required downstream notice

Anyone redistributing a generated warehouse, export, or analysis must determine
which source terms apply, retain the required attributions above, disclose
material transformations, exclude personal or third-party material not covered
by the source licence, and avoid implying government or provider endorsement.
Control of a local file does not transfer rights in its contents.
