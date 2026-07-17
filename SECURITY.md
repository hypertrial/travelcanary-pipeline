# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| 0.3.x | Yes |

Do not open a public issue for a vulnerability. Use GitHub Private
Vulnerability Reporting from the repository Security tab. If that is
unavailable, contact the maintainers through Hypertrial's standard security
contact process.

Include impact, reproduction steps, affected versions, and any proposed
mitigation. Remove credentials, `.env` values, source payloads, local warehouse
contents, and sensitive live-readiness output.

TravelCanary fetches public sources into an operator-controlled local DuckDB.
Never include source payloads, signed URLs, or warehouse contents in a report.
