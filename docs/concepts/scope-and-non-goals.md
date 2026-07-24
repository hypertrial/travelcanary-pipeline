# Scope and non-goals

TravelCanary is MIT-licensed, local-first travel-risk pipeline software. This
page is the short human summary. The authoritative licence and third-party
boundary is
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
For the operator checklist, see
[Operator responsibilities](operator-responsibilities.md).

## What This Repository Ships

- Source code, dbt models, Dagster jobs, operator scripts, and documentation.
- Five official issuer adapters: US, Canada, UK, Netherlands, and Japan.
- Required GDELT 1 daily Events context.
- Country-only DuckDB marts, optional Parquet exports, and a history
  export/import bridge across breaking warehouse rebuilds.

## What It Does Not Ship Or Operate

- No hosted continuous live ingestion or hosted travel-risk API operated by
  Hypertrial.
- No TravelCanary safety score, ranking, or travel recommendation.
- No regional public models, calibrated scoring, GDELT 2, or additional issuer
  adapters in the current release.
- No bundled production datasets in the canonical repository.

## Operator Ownership

Every operator supplies source access, runs ingestion, and stores results in
their own DuckDB file. Operators remain responsible for their inputs, outputs,
and redistribution under third-party terms.

## Related Pages

- [Operator responsibilities](operator-responsibilities.md)
- [Source coverage](../reference/source-coverage.md)
- [Privacy notice](https://github.com/hypertrial/travelcanary-pipeline/blob/main/PRIVACY.md)
