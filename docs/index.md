---
hide:
  - navigation
  - toc
---

<div class="tc-hero" markdown>

<div class="tc-hero__copy" markdown>

<span class="tc-eyebrow">Local-first travel-risk evidence</span>

# TravelCanary

Build inspectable country-level travel-advisory warehouses with dlt, dbt,
DuckDB, and Dagster.

Hypertrial-owned MIT software. No hosted service or TravelCanary score.
[Licence scope](concepts/scope-and-non-goals.md) ·
[Operator responsibilities](concepts/operator-responsibilities.md).

[Get started](getting-started/index.md){ .md-button .md-button--primary }
[Query the warehouse](guides/query-the-warehouse.md){ .md-button }

</div>

<div class="tc-hero__mark">
  <span>TravelCanary</span>
  <span>Pipeline</span>
</div>

</div>

<div class="tc-install" markdown>

**Start in the repository**

```bash
uv sync --locked --extra dev
```

</div>

## Start with a task

<div class="tc-task-grid" markdown>

<article class="tc-task-card" markdown>

### Analyze the data

Open a local DuckDB warehouse, start from `country_risk_overview`, and use
tested SQL recipes.

[Analysts hub](audiences/analysts.md)

</article>

<article class="tc-task-card" markdown>

### Operate the pipeline

Install the project, keep schedules disabled, complete a validated run, then
keep the warehouse healthy.

[Operators hub](audiences/operators.md)

</article>

<article class="tc-task-card" markdown>

### Contribute code

Change adapters, dbt marts, orchestration, or docs with the right quality gate.

[Contributors hub](audiences/contributors.md)

</article>

<article class="tc-task-card" markdown>

### Integrate downstream

Consume public marts and Parquet exports without inventing a TravelCanary score.

[Integrators hub](audiences/integrators.md)

</article>

</div>

## Product boundaries

TravelCanary publishes evidence, not a verdict. Version `0.5.0` covers five
official issuers plus required GDELT 1 context. It does not host datasets or
operate a travel-risk API.

[Choose a path](getting-started/choose-a-path.md), read the
[FAQ](concepts/faq.md), or review the
[architecture](concepts/architecture.md) before extending the pipeline.
