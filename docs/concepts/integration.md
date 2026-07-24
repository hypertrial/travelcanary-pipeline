# Integration

## Allowed inputs for downstream tools

| Input | Notes |
| --- | --- |
| Public `travelcanary_marts.*` | Prefer documented grains and filters |
| `make export-marts` Parquet | Portable snapshot with `manifest.json` |
| `make export-history` Parquet | History bridge only; existing rows win on import |

## Do not treat as APIs

- `*_raw` schemas and dlt metadata tables
- `travelcanary_ops` except for operator diagnosis
- Staging and intermediate dbt schemas
- Overview health fields as destination safety scores

## Versioning

Track
[CHANGELOG.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/CHANGELOG.md).
Warehouse layouts may break between releases; use the history bridge rather
than compatibility aliases.

See [Integrators](../audiences/integrators.md) and
[Data contracts](../reference/data-contracts.md).
