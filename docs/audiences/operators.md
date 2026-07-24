# Operators

Use this hub to run, validate, and maintain a local TravelCanary warehouse.
Schedules stay disabled until manual jobs and dbt builds are healthy.

## Path

1. **First run** — [Quickstart](../getting-started/index.md) (offline demo,
   then optional live path).
2. **Choose a path** — [Choose a path](../getting-started/choose-a-path.md)
   (demo vs live vs export/history).
3. **Day-two** — [Day-two operations](../guides/day-two-operations.md).
4. **Recover** — [Validate and recover](../guides/validate-and-recover.md) and
   [Troubleshooting](../guides/troubleshooting.md).

## Credentials And Inputs

| Flow | Network / credentials | Operator-local inputs |
| --- | --- | --- |
| Offline demo | None | Repository checkout only |
| Official advisories | Public government endpoints | `.env` timeouts / schedule flags |
| GDELT 1 Events | Public GDELT daily export | Optional `DUCKDB_MEMORY_LIMIT` |
| History bridge | None (local files) | Export Parquet + manifest under `EXPORT_DIR` |
| Live smoke / audit | Live sources on operator machine | Disposable `.cache/live_smoke.duckdb` |

Never commit `.env`, DuckDB files, live catalogs, or generated Parquet exports.
See [Operator responsibilities](../concepts/operator-responsibilities.md) and
[Scope and non-goals](../concepts/scope-and-non-goals.md).

## Confirm Success

After a first successful full run you should have `travelcanary.duckdb` with
relations under `travelcanary_marts` and `travelcanary_observability`. Those
local checks verify technical shape; they are not Hypertrial certification of
data rights or fitness for travel decisions. Query with
[Query the warehouse](../guides/query-the-warehouse.md) or hand off to an
[analyst](analysts.md).

## Advanced

| Topic | Page |
| --- | --- |
| Enable the daily schedule | [Enable schedules](../guides/enable-schedules.md) |
| Preserve history across rebuilds | [Preserve history across rebuilds](../guides/preserve-history-across-rebuilds.md) |
| Export public mart Parquet | [Export public marts](../guides/export-public-marts.md) |
| Configuration reference | [Configuration](../reference/configuration.md) |
| Operator scripts | [Scripts](../reference/scripts.md) |
