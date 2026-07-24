# Integrators

Use this hub when another repository or offline tool consumes TravelCanary
outputs. Public marts and Parquet exports are analytics inputs, not travel
advice or execution orders.

## Checklist

1. **Consume public surfaces only** — `travelcanary_marts.*` relations and
   documented Parquet exports. Do not treat `*_raw`, staging, intermediate, or
   ops schemas as APIs. Start with
   [Data contracts](../reference/data-contracts.md) and the
   [Data dictionary](../reference/data-dictionary.md).
2. **Pin versions** — track
   [CHANGELOG.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/CHANGELOG.md).
   Warehouse layouts may break between releases; use history export/import
   rather than compatibility aliases.
3. **Prefer documented exports** — `make export-marts` and
   `make export-history` (see [Export public marts](../guides/export-public-marts.md)).
4. **No score surface** — do not invent a TravelCanary recommendation from
   overview fields; health flags describe pipeline usability only.
5. **No hosted API** — this repository does not operate a travel-risk API.

## Boundary

- Consume public marts and documented Parquet only.
- Do not treat raw/ops/staging as APIs.
- TravelCanary is not travel advice and is not endorsed by any issuer.
- Breaking warehouse rebuilds remain allowed; preserve history with
  export/import.

See [Integration](../concepts/integration.md) and
[Scope and non-goals](../concepts/scope-and-non-goals.md).
