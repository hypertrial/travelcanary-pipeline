# Enable schedules

The built-in schedule targets `travelcanary_full_pipeline` at `08:00 UTC` and
is **stopped by default**.

## When to enable

1. Complete a successful manual `travelcanary_full_pipeline` materialization.
2. Inspect `travelcanary_observability.source_health` and gap models.
3. Confirm `uv run make source-audit` is healthy on an operator-owned machine.
4. Only then set `TRAVELCANARY_DAILY_SCHEDULE_ENABLED=true` in `.env` and
   restart Dagster.

## Disable again

Set `TRAVELCANARY_DAILY_SCHEDULE_ENABLED=false` (or remove the override) and
restart Dagster. Do not leave the schedule enabled while diagnosing rejected
required sources.

See [Configuration](../reference/configuration.md) and
[Day-two operations](day-two-operations.md).
