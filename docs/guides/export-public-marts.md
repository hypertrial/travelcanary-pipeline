# Export public marts

After a successful dbt build (or `make demo`), export every public mart to
Parquet for portable analysis outside the repository tooling:

```bash
uv run make export-marts
uv run python scripts/export_public_marts.py --output-dir /tmp/travelcanary-exports
```

The default directory is `EXPORT_DIR` (`exports/` under the repository root).
Each mart becomes `<mart>.parquet`, and `manifest.json` records the package
version, UTC export timestamp, per-mart row counts, and contracted column
lists. Export opens the warehouse read-only and fails if a mart is missing or
its columns drift from `PUBLIC_MART_COLUMNS`. `make export-history` uses the
same `EXPORT_DIR` default (`country_travel_risk_history.parquet` plus a sibling
manifest). Generated Parquet files are gitignored; do not commit them.

## Release assets

Version tags publish offline demo Parquet exports of the public marts as GitHub
release assets. These files are built from the seeded demo warehouse
(`.cache/travelcanary_demo.duckdb`) and are synthetic demonstration data, not
live advisories. Download them from the GitHub release page for the matching
`v*` tag, or rebuild locally with `make demo` followed by `make export-marts`.

For multi-day history across rebuilds, see
[Preserve history across rebuilds](preserve-history-across-rebuilds.md).
