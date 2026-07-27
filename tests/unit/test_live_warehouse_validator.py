from __future__ import annotations

import pytest
from scripts.validate_live_warehouse import (
    LiveWarehouseValidationError,
    validate_live_warehouse,
)

from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.public_contracts import (
    LIVE_NONEMPTY_PUBLIC_MARTS,
    LIVE_PUBLIC_MARTS,
    PUBLIC_MART_COLUMNS,
)
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection

_INTEGER_COLUMNS = {
    "gdelt_event_count_1d",
    "gdelt_event_count_7d",
    "gdelt_material_conflict_events_1d",
    "gdelt_material_conflict_events_7d",
    "reporting_issuer_count",
    "normalized_ordinal",
    "normalized_ordinal_min",
    "normalized_ordinal_max",
    "normalized_ordinal_range",
    "changed_issuer_count",
    "worsening_issuer_count",
    "improving_issuer_count",
    "matched_theme_count",
    "matched_keyword_count",
    "context_alert_count",
    "required_source_count",
    "usable_required_source_count",
    "event_count",
    "mention_count",
    "material_conflict_events",
    "hours_since_latest_run",
    "fetched_rows",
    "previous_accepted_rows",
}


def _column_type(column: str) -> str:
    if column in _INTEGER_COLUMNS:
        return "integer"
    if (
        column.endswith("_share")
        or column.startswith("gdelt_avg_")
        or column.endswith("_ratio")
    ):
        return "double"
    if (
        column.startswith("is_")
        or column.startswith("has_")
        or column.startswith("all_")
        or column.endswith("_usable")
        or column == "gdelt_is_fresh"
    ):
        return "boolean"
    return "varchar"


def _row_values(columns: list[str]) -> list[object]:
    values: list[object] = []
    for column in columns:
        col_type = _column_type(column)
        if column == "gdelt_event_count_1d":
            values.append(1)
        elif column == "gdelt_event_count_7d":
            values.append(7)
        elif col_type == "integer":
            values.append(1)
        elif col_type == "double":
            values.append(0.1)
        elif col_type == "boolean":
            values.append(True)
        else:
            values.append("x")
    return values


def _create_contract_mart(conn, mart: str, *, insert_row: bool) -> None:
    columns = PUBLIC_MART_COLUMNS[mart]
    column_sql = ", ".join(f'"{column}" {_column_type(column)}' for column in columns)
    conn.execute(f"create table travelcanary_marts.{mart} ({column_sql})")
    if insert_row:
        placeholders = ", ".join("?" for _ in columns)
        quoted = ", ".join(f'"{column}"' for column in columns)
        conn.execute(
            f"insert into travelcanary_marts.{mart} ({quoted}) values ({placeholders})",
            _row_values(columns),
        )


def _seed_validator_relations(conn):
    contracts = load_source_contracts()
    for source, contract in contracts.items():
        if contract.role != "required":
            continue
        conn.execute(
            """
            insert into travelcanary_ops.source_sync_runs values
            (?, ?, 'required', 'accepted', current_timestamp, current_timestamp,
             ?, null, ?, null, 1.0, 1.0, null)
            """,
            [
                f"run-{source}",
                source,
                max(contract.minimum_rows, 1),
                contract.minimum_rows,
            ],
        )

    conn.execute("create schema if not exists travelcanary_marts")
    nonempty = set(LIVE_NONEMPTY_PUBLIC_MARTS)
    for mart in LIVE_PUBLIC_MARTS:
        _create_contract_mart(conn, mart, insert_row=mart in nonempty)

    conn.execute("create schema if not exists travelcanary_observability")
    conn.execute(
        """
        create table travelcanary_observability.source_health (
            source varchar,
            role varchar,
            health_status varchar
        )
        """
    )
    conn.executemany(
        "insert into travelcanary_observability.source_health values (?, ?, ?)",
        [(source, contract.role, "healthy") for source, contract in contracts.items()],
    )
    conn.commit()


def test_validate_live_warehouse_accepts_seeded_ready_warehouse():
    conn = get_persistent_connection()
    try:
        _seed_validator_relations(conn)

        result = validate_live_warehouse(conn)

        assert result["required_accepted_sources"] >= 1
        assert result["public_marts_checked"] == 9
        assert result["gdelt_context_rows"] == 1
    finally:
        conn.close()


def test_validate_live_warehouse_rejects_column_drift():
    conn = get_persistent_connection()
    try:
        _seed_validator_relations(conn)
        conn.execute("drop table travelcanary_marts.country_travel_risk")
        conn.execute("create table travelcanary_marts.country_travel_risk (id integer)")
        conn.execute("insert into travelcanary_marts.country_travel_risk values (1)")

        with pytest.raises(LiveWarehouseValidationError, match="columns drifted"):
            validate_live_warehouse(conn)
    finally:
        conn.close()


def test_validate_live_warehouse_collects_missing_mart_errors():
    conn = get_persistent_connection()
    try:
        _seed_validator_relations(conn)
        conn.execute("drop table travelcanary_marts.country_travel_risk")

        with pytest.raises(
            LiveWarehouseValidationError, match="country_travel_risk is missing"
        ):
            validate_live_warehouse(conn)
    finally:
        conn.close()


def test_validate_live_warehouse_rejects_required_source_health_problem():
    conn = get_persistent_connection()
    try:
        _seed_validator_relations(conn)
        source = next(
            source
            for source, contract in load_source_contracts().items()
            if contract.role == "required"
        )
        conn.execute(
            """
            update travelcanary_observability.source_health
            set health_status = 'stale'
            where source = ?
            """,
            [source],
        )

        with pytest.raises(LiveWarehouseValidationError, match="source_health"):
            validate_live_warehouse(conn)
    finally:
        conn.close()
