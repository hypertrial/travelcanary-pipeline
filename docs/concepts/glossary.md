# Glossary

| Term | Meaning |
| --- | --- |
| `source_run_id` | UUID for one fetch/parse/guard attempt |
| Accepted / rejected / error | Ledger outcomes in `travelcanary_ops.source_sync_runs` |
| Normalized ordinal | Best-effort 1–4 approximation of a native advisory level |
| `writer.lock` | Fail-fast sibling lock serializing warehouse writers |
| Same-directory candidate | Temporary DuckDB file used for atomic dbt publish |
| `destination_iso3` | Canonical ISO3 country key in public marts |
| Required source | Any of the five official catalogs or GDELT; failure stops the full pipeline |
| Overview health | Pipeline usability fields on `country_risk_overview`, not destination safety |
| History bridge | `export-history` / `import-history` for `country_travel_risk_history` |

## See Also

- [Data dictionary](../reference/data-dictionary.md)
- [Data contracts](../reference/data-contracts.md)
- [Naming](../reference/naming.md)
