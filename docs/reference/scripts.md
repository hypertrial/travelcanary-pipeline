# Scripts

Operator scripts under `scripts/`:

| Script | Purpose |
| --- | --- |
| `audit_live_sources.py` | Read-only live source audit (`make source-audit`) |
| `run_live_smoke.py` | Disposable live Dagster path (`make live-smoke`) |
| `export_public_marts.py` | Export public marts to Parquet (`make export-marts`) |
| `export_history.py` | Export `country_travel_risk_history` (`make export-history`) |
| `import_history.py` | Import history Parquet (`make import-history`) |
| `run_atomic_dbt_build.py` | Locked candidate dbt build/publish |
| `seed_ci_warehouse.py` | Seed disposable CI warehouse |
| `validate_ci_warehouse.py` | Validate CI warehouse shape |
| `validate_live_warehouse.py` | Validate live-smoke warehouse |
| `run_dbt_unit.py` | dbt unit-test helper |

Prefer Make targets from [AGENTS.md](https://github.com/hypertrial/travelcanary-pipeline/blob/main/AGENTS.md)
over invoking scripts with ad-hoc flags unless diagnosing a source.
