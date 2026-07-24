# Operator responsibilities

This page is the operational checklist for legal and distribution hygiene.
The authoritative licence and third-party boundary remains
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
This page does not grant rights, certify compliance, or interpret third-party
terms.

## Authority

TravelCanary is MIT-licensed software and documentation. The MIT grant covers
Hypertrial-authored code and docs. It does **not** grant rights in data an
operator obtains, generates, or supplies.

## Operator Checklist

- Confirm you are authorized to access and use each source you configure
  (official issuer endpoints and GDELT).
- Keep DuckDB files, live catalogs, and generated Parquet exports
  operator-local and untracked.
- Never commit `.env` or credentials.
- Treat redistribution of warehouses, exports, and derived outputs as your
  responsibility under third-party terms that apply to you.
- Run `source-audit` and `live-smoke` on an operator-owned machine before
  tagging a release; GitHub Actions never runs live-source audits or live
  ingestion.

## Not Advice

- Documentation, SQL examples, and marts are analytics tooling. They are not
  travel advice and are not endorsed by any government or provider.
- Always consult current official advice and inspect native labels and source
  URLs before acting.
- Technical success of a local run is not Hypertrial certification of data
  rights or fitness for travel decisions.

## Privacy And Telemetry

The software has no telemetry and sends no user data to Hypertrial. Public
source providers may independently log requests. See
[PRIVACY.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/PRIVACY.md).

## Related Pages

- [Scope and non-goals](scope-and-non-goals.md)
- [Legal pointers](https://github.com/hypertrial/travelcanary-pipeline/blob/main/THIRD_PARTY_NOTICES.md)
