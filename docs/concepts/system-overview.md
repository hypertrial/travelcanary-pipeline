# System overview

TravelCanary is local-first travel-risk pipeline software. Operators supply
source access, run ingestion into their own DuckDB warehouse, and may export
public mart Parquet for offline analysis. Hypertrial operates no hosted
travel-risk API or continuous live ingestion service.

```text
Official catalogs + GDELT 1
  -> guarded raw runs and acceptance ledger
  -> dbt candidate build
  -> travelcanary_marts + travelcanary_observability
  -> optional Parquet exports / history bridge
```

## Product law

- Publish transparent issuer evidence and contextual GDELT activity.
- Do **not** produce a TravelCanary safety score or travel recommendation.
- Preserve corrected daily history in `country_travel_risk_history`.
- Fail closed on required sources; keep last-known-good marts when dbt fails.

## Related pages

- [Architecture](architecture.md)
- [Scope and non-goals](scope-and-non-goals.md)
- [Operator responsibilities](operator-responsibilities.md)
- [Integration](integration.md)
