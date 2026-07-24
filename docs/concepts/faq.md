# FAQ

| Role | Start here |
| --- | --- |
| Analyst | [Analysts hub](../audiences/analysts.md) |
| Operator | [Operators hub](../audiences/operators.md) |
| Contributor | [Contributors hub](../audiences/contributors.md) |
| Integrator | [Integrators hub](../audiences/integrators.md) |

## Is TravelCanary a hosted service?

No. Hypertrial operates no hosted travel-risk API or continuous live ingestion.
Operators run the software locally.

## Does TravelCanary produce a safety score?

No. Public marts publish transparent issuer evidence and contextual GDELT
activity. They do not produce a TravelCanary recommendation.

## Are schedules enabled by default?

No. The daily schedule is stopped unless
`TRAVELCANARY_DAILY_SCHEDULE_ENABLED=true` after a validated manual run.

## How do I keep history across a warehouse rebuild?

Use `make export-history` before deleting the warehouse, then
`make import-history` after the clean rebuild, then `make dbt-build` so change
and trend marts regenerate from restored history. Existing warehouse rows win
on import.

## Does GitHub Actions run live audits?

No. GitHub Actions never runs live-source audits or live ingestion.
`source-audit` and `live-smoke` are required before tagging a release and must
run on an operator-owned machine.

## Is this travel advice?

No. Always consult current official advice. See
[Operator responsibilities](operator-responsibilities.md).
