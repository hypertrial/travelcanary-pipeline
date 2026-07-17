# Configuration

`.env.example` is the executable inventory for local configuration.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DUCKDB_PATH` | unset | Absolute warehouse path; overrides `DUCKDB_NAME` |
| `DUCKDB_NAME` | `travelcanary.duckdb` | Warehouse filename under the repository root |
| `DBT_PROFILES_DIR` | repository profile | Optional dbt profiles directory |
| `HTTP_CONNECT_TIMEOUT_SECONDS` | `10` | Outbound connection timeout |
| `HTTP_READ_TIMEOUT_SECONDS` | `30` | Outbound read timeout |
| `GDELT_ROLLING_WINDOW_DAYS` | `120` | Accepted raw GDELT retention |
| `TRAVELCANARY_DAILY_SCHEDULE_ENABLED` | `false` | Start the daily 08:00 UTC schedule |
| `UK_FCDO_DISCOVER_ALL_COUNTRIES` | `true` | Discover the complete GOV.UK catalog |

Source contract thresholds live in `dbt/seeds/source_contracts.csv`, not environment variables.

Configuration is validated when settings are imported, before network or warehouse work begins. Integer values must be positive integers. Boolean values accept only `1`, `true`, `yes`, or `on` and `0`, `false`, `no`, or `off`, case-insensitively; blank or malformed booleans fail startup. Blank optional strings are treated as unset. Blank `DUCKDB_PATH` and `DUCKDB_NAME` values therefore select the default repository warehouse; the resolved target must be a file, and an existing directory fails startup. A blank `DBT_PROFILES_DIR` uses the repository profile, while a nonblank path must contain a parseable `profiles.yml` with a valid `travelcanary` DuckDB target, output, and path or startup fails with an actionable error.
