# TravelCanary

TravelCanary is a local-first data product for analysts and developers who need to compare official travel advisories without hiding their meaning behind a proprietary score.

The pipeline lands source catalogs with dlt, transforms and tests them with dbt,
stores everything in one operator-controlled DuckDB file, and exposes lineage and
scheduling through Dagster. Public marts are country-only and retain both
source-native meaning and a clearly labeled best-effort 1–4 approximation.
Analysts can start with `country_risk_overview`, then drill into the dedicated
advisory, history, theme, event, alert, and quality marts.

## Product boundaries

TravelCanary publishes evidence, not a verdict. It provides issuer reporting counts, normalized minimum/median/maximum/range, advisory changes, country trends, advisory text themes, GDELT event context, divergence flags, and source quality. It does not claim that issuers are calibrated to one another, create a TravelCanary risk score, or recommend whether someone should travel.

The `0.4.0` scope adds portable Parquet exports of the public marts and a
history export/re-import path that bridges breaking warehouse rebuilds. It
still deliberately excludes regional public models, hosted delivery, APIs,
calibrated scoring, GDELT 2, and additional issuer adapters.

## Guarantees

- Five required official catalogs are fetched as complete batches and guarded before landing.
- GDELT 1 complete daily Events is required for the full pipeline.
- Rejected or uncommitted run IDs cannot become current public data.
- Every public destination resolves through the committed ISO country dimension.
- Daily advisory history is retained indefinitely, and a same-UTC-day rerun corrects that day.

TravelCanary is not travel advice and is not endorsed by any government or
data provider. Travel advisories remain time-sensitive source statements.
Always consult current official advice and inspect the cited source URL and
native label before acting.

The software has no telemetry and sends no user data to Hypertrial. Upstream
providers may independently log requests. MIT applies only to Hypertrial's
original code; review the
[canonical third-party notices](https://github.com/hypertrial/travelcanary-pipeline/blob/main/THIRD_PARTY_NOTICES.md)
and [privacy notice](https://github.com/hypertrial/travelcanary-pipeline/blob/main/PRIVACY.md).
