from __future__ import annotations

import pytest
from scripts.validate_live_warehouse import (
    LiveWarehouseValidationError,
    validate_live_warehouse,
)

from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection


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
    conn.execute("create table travelcanary_marts.country_travel_risk (id integer)")
    conn.execute("insert into travelcanary_marts.country_travel_risk values (1)")
    conn.execute(
        """
        create table travelcanary_marts.country_risk_signals (
            gdelt_event_count_1d integer,
            gdelt_event_count_7d integer
        )
        """
    )
    conn.execute("insert into travelcanary_marts.country_risk_signals values (1, 7)")
    conn.execute("create table travelcanary_marts.country_risk_overview (id integer)")
    conn.execute("insert into travelcanary_marts.country_risk_overview values (1)")
    conn.execute(
        "create table travelcanary_marts.country_advisory_changes (id integer)"
    )
    conn.execute("insert into travelcanary_marts.country_advisory_changes values (1)")
    conn.execute("create table travelcanary_marts.country_risk_trends (id integer)")
    conn.execute("insert into travelcanary_marts.country_risk_trends values (1)")
    conn.execute("create table travelcanary_marts.country_advisory_themes (id integer)")
    conn.execute(
        "create table travelcanary_marts.country_gdelt_event_types (id integer)"
    )
    conn.execute("insert into travelcanary_marts.country_gdelt_event_types values (1)")
    conn.execute("create table travelcanary_marts.country_context_alerts (id integer)")
    conn.execute("create table travelcanary_marts.source_data_quality (id integer)")
    conn.execute("insert into travelcanary_marts.source_data_quality values (1)")

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
