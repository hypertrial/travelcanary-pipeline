import io
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from travelcanary_pipeline.config.settings import BASE_DIR
from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.ingestion.gdelt.events import (
    _load_fips_codes,
    _safe_float,
    _safe_int,
    iter_gdelt_export_zip,
    sync_gdelt_daily_events,
    upsert_gdelt_events,
)
from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.naming import SOURCE_GDELT, raw_schema
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "gdelt" / "20260701.export.CSV"
)


def _zip_with_tsv(line: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("20260701.export.CSV", line)
    return buf.getvalue()


def test_iter_gdelt_export_zip_reads_tab_delimited_row():
    row = list(
        iter_gdelt_export_zip(
            io.BytesIO(_zip_with_tsv(FIXTURE.read_text(encoding="utf-8").strip())),
            ingested_at="2026-07-01T00:00:00+00:00",
        )
    )
    assert row[0]["global_event_id"] == "900000001"
    assert row[0]["action_geo_country_code"] == "TH"


def test_iter_gdelt_export_zip_is_lazy():
    rows = iter_gdelt_export_zip(io.BytesIO(b"not a zip"), ingested_at="now")

    assert iter(rows) is rows
    with pytest.raises(zipfile.BadZipFile):
        next(rows)


def test_iter_gdelt_export_zip_rejects_wrong_field_count():
    diagnostics = BatchDiagnostics()
    assert (
        list(
            iter_gdelt_export_zip(
                io.BytesIO(_zip_with_tsv("too\tfew\tfields")),
                ingested_at="2026-07-01T00:00:00+00:00",
                diagnostics=diagnostics,
            )
        )
        == []
    )
    assert diagnostics.summary() == {
        "discovered_rows": 1,
        "skipped_rows": 1,
        "blocking_skipped_rows": 1,
        "skip_reasons": {"malformed export row": 1},
    }


def test_gdelt_parser_rejects_zip_without_export_and_safely_casts_values():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("readme.txt", "not an export")
    with pytest.raises(ValueError, match="missing CSV"):
        list(iter_gdelt_export_zip(io.BytesIO(buffer.getvalue()), ingested_at="now"))

    assert _safe_int("") is None
    assert _safe_int("bad") is None
    assert _safe_float(None) is None
    assert _safe_float("bad") is None


def test_gdelt_empty_upsert_and_fips_seed_load():
    conn = Mock()
    assert upsert_gdelt_events(conn, []) == 0
    assert "TH" in _load_fips_codes()
    assert (BASE_DIR / "dbt" / "seeds" / "fips10_4_to_iso3.csv").exists()


def test_sync_gdelt_daily_events_owns_and_closes_connection():
    conn = Mock()
    conn.execute.return_value.fetchone.side_effect = [(0,), (0,)]

    with (
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events.iter_gdelt_export_zip",
            return_value=iter(()),
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.connection.ensure_duck_db",
            return_value=None,
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.connection.get_persistent_connection",
            return_value=conn,
        ),
    ):
        result = sync_gdelt_daily_events(
            fetch_file=lambda _url, destination: destination.write(b"zip"),
            enforce_contract=False,
        )

    assert result["rows"] == 0
    conn.close.assert_called_once()


def test_sync_gdelt_daily_events_records_error_on_transaction_failure():
    conn = Mock()

    with (
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events.iter_gdelt_export_zip",
            return_value=iter([{"action_geo_country_code": "TH", "source_run_id": ""}]),
        ),
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events._load_fips_codes",
            return_value={"TH"},
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.source_runs.previous_accepted_rows",
            return_value=None,
        ),
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events.upsert_gdelt_events",
            side_effect=RuntimeError("upsert failed"),
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.source_runs.record_source_run_status",
        ),
        pytest.raises(RuntimeError, match="upsert failed"),
    ):
        sync_gdelt_daily_events(
            fetch_file=lambda _url, destination: destination.write(b"zip"), conn=conn
        )

    conn.rollback.assert_called_once()


def test_sync_gdelt_daily_events_records_download_failure_before_transaction():
    conn = Mock()

    with (
        patch(
            "travelcanary_pipeline.storage.duckdb.source_runs.previous_accepted_rows",
            return_value=None,
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.source_runs.record_source_run_status",
        ) as record,
        pytest.raises(RuntimeError, match="download failed"),
    ):
        sync_gdelt_daily_events(
            fetch_file=lambda _url, _destination: (_ for _ in ()).throw(
                RuntimeError("download failed")
            ),
            conn=conn,
        )

    assert record.call_args.kwargs["status"] == "error"
    assert record.call_args.kwargs["fetched_rows"] == 0
    conn.commit.assert_called_once()
    conn.execute.assert_not_called()


def test_sync_gdelt_daily_events_reraises_unguarded_download_failure():
    conn = Mock()
    with (
        patch(
            "travelcanary_pipeline.storage.duckdb.source_runs.record_source_run_status"
        ) as record,
        pytest.raises(RuntimeError, match="download failed"),
    ):
        sync_gdelt_daily_events(
            fetch_file=lambda _url, _destination: (_ for _ in ()).throw(
                RuntimeError("download failed")
            ),
            conn=conn,
            enforce_contract=False,
        )

    record.assert_not_called()
    conn.commit.assert_not_called()
    conn.execute.assert_not_called()


def test_sync_gdelt_daily_events_reraises_error_without_contract_recording():
    conn = Mock()

    with (
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events.iter_gdelt_export_zip",
            return_value=iter([{"action_geo_country_code": "TH", "source_run_id": ""}]),
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.source_runs.record_source_run_status",
        ) as record,
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events.upsert_gdelt_events",
            side_effect=RuntimeError("upsert failed"),
        ),
        pytest.raises(RuntimeError, match="upsert failed"),
    ):
        sync_gdelt_daily_events(
            fetch_file=lambda _url, destination: destination.write(b"zip"),
            conn=conn,
            enforce_contract=False,
        )

    conn.rollback.assert_called_once()
    record.assert_not_called()


def test_gdelt_malformed_row_rolls_back_and_records_rejection():
    contract = load_source_contracts()[SOURCE_GDELT]
    permissive = contract.__class__(**{**contract.__dict__, "minimum_rows": 1})
    payload = _zip_with_tsv(f"{FIXTURE.read_text(encoding='utf-8').strip()}\ntoo\tfew")
    conn = get_persistent_connection()
    try:
        with (
            patch(
                "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
                return_value={SOURCE_GDELT: permissive},
            ),
            pytest.raises(ValueError, match="malformed source records"),
        ):
            sync_gdelt_daily_events(
                fetch_file=lambda _url, destination: destination.write(payload),
                conn=conn,
            )

        assert conn.execute(
            f"select count(*) from {raw_schema(SOURCE_GDELT)}.events"
        ).fetchone() == (0,)
        assert conn.execute(
            "select status from travelcanary_ops.source_sync_runs"
        ).fetchone() == ("rejected",)
    finally:
        conn.close()


def test_unguarded_gdelt_malformed_row_still_rolls_back():
    payload = _zip_with_tsv(f"{FIXTURE.read_text(encoding='utf-8').strip()}\ntoo\tfew")
    conn = get_persistent_connection()
    try:
        with pytest.raises(ValueError, match="malformed source records"):
            sync_gdelt_daily_events(
                fetch_file=lambda _url, destination: destination.write(payload),
                conn=conn,
                enforce_contract=False,
            )
        assert conn.execute(
            f"select count(*) from {raw_schema(SOURCE_GDELT)}.events"
        ).fetchone() == (0,)
    finally:
        conn.close()
