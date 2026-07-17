from __future__ import annotations

import json
from datetime import date

import duckdb
import pytest
from scripts import audit_live_sources as audit_cli

from travelcanary_pipeline import live_audit as audit
from travelcanary_pipeline.ingestion.source_contracts import SourceContract
from travelcanary_pipeline.naming import (
    SOURCE_GDELT,
    SOURCE_US_STATE,
)


def _contract(source: str, role: str = "required", minimum_rows: int = 1):
    return SourceContract(
        source=source,
        role=role,
        minimum_rows=minimum_rows,
        warn_drop_ratio=0.9,
        reject_drop_ratio=0.8,
        warn_after_hours=36,
        error_after_hours=72,
        minimum_canonical_ratio=0.98,
        warn_canonical_ratio=0.99,
        minimum_normalization_ratio=0.99 if role == "required" else 0.0,
    )


def _row() -> dict[str, object]:
    return {"destination_iso2": "TH", "native_level": "1"}


def _rows(count: int) -> list[dict[str, object]]:
    return [_row() for _ in range(count)]


def _patch_common(monkeypatch, contracts):
    monkeypatch.setattr(audit, "load_source_contracts", lambda: contracts)
    monkeypatch.setattr(audit, "canonical_country_ratio", lambda _rows: 1.0)
    monkeypatch.setattr(audit, "normalization_ratio", lambda _source, _rows: 1.0)


def _write_previous_run(warehouse, source: str, fetched_rows: int) -> None:
    conn = duckdb.connect(str(warehouse))
    try:
        conn.execute("CREATE SCHEMA travelcanary_ops")
        conn.execute(
            """
            CREATE TABLE travelcanary_ops.source_sync_runs (
                source_run_id VARCHAR,
                source VARCHAR,
                role VARCHAR,
                status VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                fetched_rows BIGINT,
                previous_accepted_rows BIGINT,
                minimum_rows BIGINT,
                relative_ratio DOUBLE,
                canonical_ratio DOUBLE,
                normalization_ratio DOUBLE,
                message VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO travelcanary_ops.source_sync_runs VALUES
            (
                'previous-run',
                ?,
                'required',
                'accepted',
                TIMESTAMP '2026-07-09 07:55:00',
                TIMESTAMP '2026-07-09 08:00:00',
                ?,
                NULL,
                1,
                NULL,
                1.0,
                1.0,
                NULL
            )
            """,
            [source, fetched_rows],
        )
    finally:
        conn.close()


def test_source_audit_filters_source_outputs_json_and_proposes_floors(
    monkeypatch, capsys
):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE)}
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(
        audit,
        "ADVISORY_SOURCES",
        (
            (
                SOURCE_US_STATE,
                lambda **_kwargs: [_row(), _row(), _row()],
                "https://example.test/us",
            ),
        ),
    )
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    exit_code = audit_cli.main(
        ["--source", SOURCE_US_STATE, "--output", "json", "--propose-floors"]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [row["source"] for row in payload] == [SOURCE_US_STATE]
    assert payload[0]["status"] == "accepted"
    assert payload[0]["rows"] == 3
    assert payload[0]["proposed_minimum_rows"] == 2
    assert payload[0]["source_url"] == "https://example.test/us"
    assert payload[0]["started_at"]
    assert payload[0]["finished_at"]
    assert payload[0]["duration_seconds"] >= 0
    assert payload[0]["discovered_rows"] == 3
    assert payload[0]["skipped_rows"] == 0
    assert payload[0]["blocking_skipped_rows"] == 0
    assert payload[0]["skip_reasons"] == {}


def test_source_audit_warehouse_context_populates_previous_rows(
    monkeypatch, capsys, tmp_path
):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE)}
    warehouse = tmp_path / "warehouse.duckdb"
    _write_previous_run(warehouse, SOURCE_US_STATE, 4)
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(
        audit,
        "ADVISORY_SOURCES",
        ((SOURCE_US_STATE, lambda **_kwargs: _rows(4), "https://example.test/us"),),
    )
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    exit_code = audit_cli.main(
        [
            "--source",
            SOURCE_US_STATE,
            "--warehouse",
            str(warehouse),
            "--output",
            "json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "accepted"
    assert payload[0]["previous_context_status"] == "available"
    assert payload[0]["previous_accepted_rows"] == 4
    assert payload[0]["previous_accepted_finished_at"] == "2026-07-09T08:00:00"
    assert payload[0]["relative_ratio"] == 1.0


def test_source_audit_warehouse_context_enables_relative_drop_rejection(
    monkeypatch, capsys, tmp_path
):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE, minimum_rows=1)}
    warehouse = tmp_path / "warehouse.duckdb"
    _write_previous_run(warehouse, SOURCE_US_STATE, 4)
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(
        audit,
        "ADVISORY_SOURCES",
        ((SOURCE_US_STATE, lambda **_kwargs: _rows(1), "https://example.test/us"),),
    )
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    exit_code = audit_cli.main(
        [
            "--source",
            SOURCE_US_STATE,
            "--warehouse",
            str(warehouse),
            "--output",
            "json",
        ]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "rejected"
    assert payload[0]["previous_context_status"] == "available"
    assert payload[0]["previous_accepted_rows"] == 4
    assert payload[0]["relative_ratio"] == 0.25
    assert "row count ratio 0.250 below 0.800" in payload[0]["reason"]


def test_source_audit_missing_warehouse_context_is_nonblocking(
    monkeypatch, capsys, tmp_path
):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE)}
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(
        audit,
        "ADVISORY_SOURCES",
        ((SOURCE_US_STATE, lambda **_kwargs: [_row()], "https://example.test/us"),),
    )
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    exit_code = audit_cli.main(
        [
            "--source",
            SOURCE_US_STATE,
            "--warehouse",
            str(tmp_path / "missing.duckdb"),
            "--output",
            "json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "accepted"
    assert payload[0]["previous_context_status"] == "unavailable"
    assert "warehouse not found" in payload[0]["previous_context_reason"]


def test_source_audit_uninitialized_warehouse_context_is_nonblocking(
    monkeypatch, capsys, tmp_path
):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE)}
    warehouse = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(warehouse))
    conn.close()
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(
        audit,
        "ADVISORY_SOURCES",
        ((SOURCE_US_STATE, lambda **_kwargs: [_row()], "https://example.test/us"),),
    )
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    exit_code = audit_cli.main(
        [
            "--source",
            SOURCE_US_STATE,
            "--warehouse",
            str(warehouse),
            "--output",
            "json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "accepted"
    assert payload[0]["previous_context_status"] == "unavailable"
    assert (
        payload[0]["previous_context_reason"]
        == "source_sync_runs not found in warehouse"
    )


def test_source_audit_text_output(monkeypatch, capsys):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE)}
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(
        audit,
        "ADVISORY_SOURCES",
        ((SOURCE_US_STATE, lambda **_kwargs: [_row()], "https://example.test/us"),),
    )
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    assert audit_cli.main(["--source", SOURCE_US_STATE, "--output", "text"]) == 0

    text = capsys.readouterr().out
    assert f"source={SOURCE_US_STATE}" in text
    assert "status=accepted" in text
    assert "source_url=https://example.test/us" in text


def test_source_audit_invalid_source_fails_fast(monkeypatch):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE)}
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    with pytest.raises(SystemExit) as exc:
        audit_cli.main(["--source", "not_a_source"])

    assert exc.value.code == 2


def test_source_audit_required_rejection_returns_nonzero(monkeypatch, capsys):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE, minimum_rows=1)}
    _patch_common(monkeypatch, contracts)
    monkeypatch.setattr(
        audit,
        "ADVISORY_SOURCES",
        ((SOURCE_US_STATE, lambda **_kwargs: [], "https://example.test/us"),),
    )
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_US_STATE,))

    assert audit_cli.main(["--source", SOURCE_US_STATE]) == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "rejected"
    assert "below floor" in payload[0]["reason"]


def test_gdelt_audit_streams_download_and_counts_mapping(monkeypatch):
    contract = _contract(SOURCE_GDELT)
    contracts = {
        SOURCE_GDELT: contract.__class__(
            **{**contract.__dict__, "minimum_canonical_ratio": 0.4}
        )
    }
    monkeypatch.setattr(audit, "load_source_contracts", lambda: contracts)
    monkeypatch.setattr(audit, "_load_fips_codes", lambda: {"TH"})
    monkeypatch.setattr(
        audit,
        "download_to_file",
        lambda _url, destination: destination.write(b"zip"),
    )
    monkeypatch.setattr(
        audit,
        "iter_gdelt_export_zip",
        lambda _file, **_kwargs: iter(
            [
                {"action_geo_country_code": "TH"},
                {"action_geo_country_code": "XX"},
                {"action_geo_country_code": None},
            ]
        ),
    )

    result = audit._audit_gdelt(date(2026, 7, 1))

    assert result["status"] == "accepted"
    assert result["rows"] == 3
    assert result["mapped_geography_ratio"] == 0.5

    def observed_iterator(_file, *, diagnostics, **_kwargs):
        diagnostics.observe()
        return iter([{"action_geo_country_code": "TH"}])

    monkeypatch.setattr(audit, "iter_gdelt_export_zip", observed_iterator)
    assert audit._audit_gdelt(date(2026, 7, 1))["discovered_rows"] == 1


def test_live_audit_helpers_cover_selection_urls_and_errors(monkeypatch):
    contracts = {SOURCE_US_STATE: _contract(SOURCE_US_STATE)}
    assert audit.select_sources(None, contracts) == [SOURCE_US_STATE]
    ghost = _contract("ghost")
    with pytest.raises(ValueError, match="no live auditor"):
        audit.select_sources(["ghost"], {"ghost": ghost})

    assert audit._concise_previous_context_reason("database is locked") == (
        "warehouse is locked"
    )
    assert "gdelt" in audit._source_url(SOURCE_GDELT, date(2026, 7, 1))
    assert audit._load_previous_context("unused", []) == {}

    def fail():
        raise RuntimeError("audit failed")

    result = audit._run_with_timing(
        SOURCE_US_STATE,
        contracts[SOURCE_US_STATE],
        "https://example.test",
        {},
        fail,
    )
    assert result["status"] == "error"
    assert result["reason"] == "audit failed"


def test_audit_sources_routes_gdelt(monkeypatch):
    contracts = {SOURCE_GDELT: _contract(SOURCE_GDELT)}
    monkeypatch.setattr(audit, "load_source_contracts", lambda: contracts)
    monkeypatch.setattr(audit, "AUDIT_SOURCE_ORDER", (SOURCE_GDELT,))
    monkeypatch.setattr(
        audit,
        "_audit_gdelt",
        lambda *_args, **_kwargs: {
            "source": SOURCE_GDELT,
            "role": "required",
            "status": "accepted",
            "rows": 1,
        },
    )
    progress: list[str] = []

    results = audit.audit_sources(
        date(2026, 7, 1),
        selected_sources=contracts,
        progress=progress.append,
    )

    assert [row["source"] for row in results] == [SOURCE_GDELT]
    assert progress == [
        "auditing gdelt for 2026-07-01",
        "gdelt: accepted",
    ]
    assert (
        len(
            audit.audit_sources(
                date(2026, 7, 1),
                selected_sources=[SOURCE_GDELT],
            )
        )
        == 1
    )
