# Contributors

Use this hub when changing adapters, dbt models, docs, or orchestration. For
operator setup, start with [Quickstart](../getting-started/index.md).

## Setup

```bash
cp .env.example .env
uv sync --locked --extra dev
```

Keep schedules disabled unless you intentionally test live ingestion. Docs
contributors should install Chromium for Playwright render checks once:

```bash
uv run playwright install chromium
```

## Which Quality Gate?

| Change | Gate |
| --- | --- |
| Docs, styles, or `mkdocs.yml` only | `uv run make docs-check` |
| Ordinary code or test PR | Offline release gate in AGENTS.md / CONTRIBUTING |
| Pre-release | Full offline gate plus local `source-audit` and `live-smoke` |

The full command tables live in
[AGENTS.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/AGENTS.md).
Do not duplicate them elsewhere.

## Contribution Checklists

See [Development](../development/index.md) for fixture policy, golden-row
rules, and contract-change requirements.

## Data And IP Hygiene

- Do not contribute production data, scraped dumps, or non-synthetic warehouse
  rows.
- Keep fixtures minimal and synthetic under `tests/fixtures/`.
- Do not invent a TravelCanary score or recommendation in public marts.
- Read [Operator responsibilities](../concepts/operator-responsibilities.md)
  and [CONTRIBUTING.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/CONTRIBUTING.md).

## Design Decisions

TravelCanary intentionally publishes evidence without a proprietary score,
uses atomic candidate dbt publish, and bridges history across rebuilds instead
of compatibility aliases. Read [Design decisions](../concepts/decisions.md)
before proposing scoring APIs or dual warehouse paths.
