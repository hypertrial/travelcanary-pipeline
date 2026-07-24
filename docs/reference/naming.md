# Naming

TravelCanary uses `<source>_<layer>_<subject>` for flat Python names and `[source, layer, subject]` for Dagster asset keys. There is no event/scope segment.

Examples:

- Python asset: `us_state_raw_advisories`
- Dagster key: `us_state / raw / advisories`
- Raw schema: `us_state_raw`
- dbt schema: `travelcanary_marts`

## Source identifiers

| Identifier | Source |
| --- | --- |
| `us_state` | US State Department |
| `ca_gac` | Global Affairs Canada |
| `uk_fcdo` | UK FCDO |
| `nl_mfa` | Netherlands MFA |
| `jp_mofa` | Japan MOFA |
| `gdelt` | GDELT Events |

Raw-source dbt metadata must use the exact custom ingestion asset key. A flattened schema token such as `us_state_raw / advisories` is a disconnected duplicate and fails the lineage test.
