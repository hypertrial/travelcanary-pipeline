"""Validate the disposable live-smoke DuckDB warehouse."""

from __future__ import annotations

from typing import Any

import duckdb

from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.public_contracts import (
    LIVE_NONEMPTY_PUBLIC_MART_RELATIONS,
    LIVE_PUBLIC_MART_RELATIONS,
)
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection


class LiveWarehouseValidationError(RuntimeError):
    """Raised when a live-smoke warehouse violates readiness checks."""


def _single_int(
    conn: duckdb.DuckDBPyConnection, sql: str, parameters: list[Any] | None = None
) -> int:
    row = conn.execute(sql, parameters or []).fetchone()
    return int(row[0] if row else 0)


def validate_live_warehouse(
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, int]:
    owns_conn = conn is None
    if conn is None:
        conn = get_persistent_connection()
    errors: list[str] = []
    try:
        required_sources = [
            source
            for source, contract in load_source_contracts().items()
            if contract.role == "required"
        ]
        for source in required_sources:
            accepted_runs = _single_int(
                conn,
                """
                select count(*)
                from travelcanary_ops.source_sync_runs
                where source = ? and status = 'accepted'
                """,
                [source],
            )
            if accepted_runs == 0:
                errors.append(f"{source} has no accepted source_sync_runs entry")

        for mart in LIVE_PUBLIC_MART_RELATIONS:
            _single_int(conn, f"select count(*) from {mart}")

        for mart in LIVE_NONEMPTY_PUBLIC_MART_RELATIONS:
            rows = _single_int(conn, f"select count(*) from {mart}")
            if rows == 0:
                errors.append(f"{mart} is empty")

        gdelt_context_rows = _single_int(
            conn,
            """
            select count(*)
            from travelcanary_marts.country_risk_signals
            where coalesce(gdelt_event_count_1d, 0) > 0
               or coalesce(gdelt_event_count_7d, 0) > 0
            """,
        )
        if gdelt_context_rows == 0:
            errors.append("country_risk_signals has no non-zero GDELT context")

        unhealthy_required = conn.execute(
            """
            select source, health_status
            from travelcanary_observability.source_health
            where role = 'required'
              and health_status in ('error', 'rejected', 'stale', 'unavailable')
            order by source
            """
        ).fetchall()
        if unhealthy_required:
            formatted = ", ".join(
                f"{source}={status}" for source, status in unhealthy_required
            )
            errors.append(f"required source_health is not clean: {formatted}")

        if errors:
            raise LiveWarehouseValidationError("; ".join(errors))
        return {
            "required_accepted_sources": len(required_sources),
            "public_marts_checked": len(LIVE_PUBLIC_MART_RELATIONS),
            "gdelt_context_rows": gdelt_context_rows,
        }
    finally:
        if owns_conn:
            conn.close()


def main() -> int:
    result = validate_live_warehouse()
    print(f"Live warehouse validated: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
