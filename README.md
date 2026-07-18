# TravelCanary pipeline

TravelCanary is a local-first **dlt + dbt + DuckDB + Dagster** pipeline for analysts and developers making sense of country-level travel-risk data.

It completes and guards five official government advisory catalogs, retains corrected daily history, and publishes transparent issuer consensus/disagreement with contextual GDELT 1 activity. It does **not** produce a TravelCanary safety score or travel recommendation.

Supported ingestion and dbt commands serialize writes to the local warehouse.
dbt builds and tests run against a same-directory candidate file and atomically
publish it only after every model and test succeeds, keeping analyst marts
last-known-good when transformation fails.

## Quickstart

```bash
cp .env.example .env
uv sync --locked --extra dev
uv run make test
uv run make demo
uv run make dagster-dev
```

`demo` creates the deterministic offline `.cache/travelcanary_demo.duckdb`
warehouse; `demo-ui` also opens it when the optional DuckDB CLI is installed.
Neither command proves that live sources are reachable. Run `uv run make
source-audit` for the read-only, opt-in live audit, add `--warehouse PATH` only
when you want read-only previous-run context from an existing warehouse, or run
`uv run make live-smoke` to exercise the live Dagster path against the
disposable `.cache/live_smoke.duckdb` warehouse. Live audits and ingestion are
local-only; GitHub Actions runs a compact offline gate. Use
`uv run make export-history` before a breaking warehouse rebuild, then
`uv run make import-history HISTORY_PATH=...` after the clean rebuild to retain
accepted advisory history.

See the [documentation](https://hypertrial.github.io/travelcanary-pipeline/),
especially the [quickstart](docs/quickstart.md), [consumer guide](docs/consumer-guide.md),
[data contracts](docs/data-contracts.md), and [operations guide](docs/operations.md).
Read the canonical [third-party and source notices](THIRD_PARTY_NOTICES.md) and
[privacy notice](PRIVACY.md) before use or redistribution.

Offline saved-payload contracts for every external adapter run inside the
default unit-coverage gate. Five critical marts also have reviewed exact-row
CSV regressions inside dbt integration coverage. Run either layer directly:

```bash
uv run make contract-http
uv run make golden-dbt
```

Fixtures contain only sanitized provider-shaped fragments and deterministic
seeded output; neither command contacts a live source.

Contributors should read [CONTRIBUTING.md](CONTRIBUTING.md). Release history is
in [CHANGELOG.md](CHANGELOG.md), and vulnerabilities should be reported through
the private process in [SECURITY.md](SECURITY.md).

## Scope

- Official: United States, Canada, United Kingdom, Netherlands, and Japan.
- Context: required GDELT 1 daily Events.
- Output: country-only DuckDB marts for local analysis, plus optional Parquet
  exports of the public marts and a history export/re-import bridge across
  breaking warehouse rebuilds.
- Deferred: regional public models, calibrated scoring, APIs, GDELT 2, and
  additional issuers.

TravelCanary is not travel advice and is not endorsed by any government or
data provider. Always consult current official advice. The software has no
telemetry and sends no user data to Hypertrial; source providers may log
requests independently.

MIT covers Hypertrial's original code only. Source content, reference
mappings, and generated outputs retain their applicable source terms. See
[LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
