# Design decisions

## Local-first, no hosted API

**Why:** Operators control warehouses and redistribution under source terms.
Hypertrial does not operate a travel-risk API.

## Evidence without a TravelCanary score

**Why:** Issuer meaning must stay inspectable. Normalized ordinals are labeled
approximations; overview health is pipeline usability only.

## Atomic candidate dbt publish

**Why:** Failed transformations must not expose partial marts. dbt builds a
same-directory candidate and promotes it only after the full build passes.

## History bridge over compatibility aliases

**Why:** Breaking warehouse rebuilds remain allowed. Preserve
`country_travel_risk_history` with export/import instead of dual schemas.

## Required GDELT 1 context

**Why:** Compact, stable daily files provide country activity context without
claiming an independent safety recommendation. GDELT 2 is deferred.

## Related Pages

- [Architecture](architecture.md)
- [Scope and non-goals](scope-and-non-goals.md)
