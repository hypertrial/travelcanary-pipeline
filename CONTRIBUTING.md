# Contributing to TravelCanary

TravelCanary is a local-first travel-risk data pipeline. Keep contributions
country-level, preserve native advisory meaning, and do not add a safety score
or recommendation to public marts.

## Setup

```bash
uv sync --locked --extra dev
cp .env.example .env
```

Keep the daily schedule disabled during development. Before opening a pull
request, install the externally managed Costguard `2.5.0` CLI:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

The Make target checks this exact version and never installs it automatically.
Then run:

```bash
uv run make lint
uv run make test-cov
uv run make contract-http
uv run make integration-dagster-cov
uv run make integration-dbt-cov
uv run make dbt-unit
uv run make golden-dbt
uv run make coverage-report
uv run make docs-check
uv run make check-secrets
uv run make dbt-parse
uv run make dbt-build-ci
uv run make costguard
uv run make demo
```

Before tagging a release, also run the mandatory local-only live checks on an
operator-owned machine:

```bash
uv run make source-audit
uv run make live-smoke
```

Create the GitHub release with `gh release create` so the `v*` tag exists before
the docs and demo Parquet release-asset workflows run. Live source checks are
local-only and must never be wired into Actions. Never commit credentials,
`.env`, DuckDB files, unsanitized source payloads, generated sites, or live
diagnostics.

Contract changes must update the authoritative seed, tests, changelog, and
source-coverage documentation together. Saved payload fixtures under
`tests/fixtures/contracts/` preserve only the minimum reviewed upstream shape
and run without network access in default CI. Update one only with an
intentional adapter compatibility change. Golden CSV changes require the same
review as a public mart contract; never regenerate and accept them blindly.

## Developer Certificate of Origin

Every commit must be signed off under the
[Developer Certificate of Origin 1.1](https://developercertificate.org/) with:

```text
Signed-off-by: Your Name <your-email@example.com>
```

Use `git commit -s` to add the sign-off. By signing off, you certify that you
have the right to submit the contribution and license it under this
repository's MIT licence. Hypertrial does not require a contributor licence
agreement.

Your name, sign-off, commit email, GitHub account, and submitted contribution
metadata become public and may be retained indefinitely. GitHub's no-reply
email option is recommended if you do not want a personal email address in
public history. Pull requests must pass the required DCO status check.

See [PRIVACY.md](PRIVACY.md) for contributor-data handling and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) before adding source-derived
material.
