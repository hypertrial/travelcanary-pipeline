# Choose a path

TravelCanary is a single product with three common operator paths.

| Path | Use when | Start here |
| --- | --- | --- |
| Offline demo | Learn marts without live sources | `uv run make demo` then [Query the warehouse](../guides/query-the-warehouse.md) |
| Live operator warehouse | Ingest official catalogs + GDELT | [Quickstart](index.md) → `source-audit` → manual full pipeline |
| Export / history bridge | Share Parquet or survive a rebuild | [Export public marts](../guides/export-public-marts.md) and [Preserve history](../guides/preserve-history-across-rebuilds.md) |

## Notes

- The offline demo always recreates `.cache/travelcanary_demo.duckdb` and never
  proves live source reachability.
- Before tagging a release, run `uv run make source-audit` and
  `uv run make live-smoke` on an operator-owned machine.
- Schedules stay disabled until a validated manual run succeeds.

See [Operators](../audiences/operators.md) for the full operator path.
