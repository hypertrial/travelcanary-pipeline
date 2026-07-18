"""Unit tests for history export and anti-join import."""

from __future__ import annotations

import duckdb
import pytest

from travelcanary_pipeline.history_transfer import (
    HISTORY_COLUMNS,
    HISTORY_RELATION,
    HistoryTransferError,
    export_history,
    import_history,
)
from travelcanary_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    get_persistent_connection,
)
from travelcanary_pipeline.storage.duckdb.writer_lock import warehouse_writer_lock


def _create_history_table(conn: duckdb.DuckDBPyConnection) -> None:
    col_defs = ", ".join(f"{name} VARCHAR" for name in HISTORY_COLUMNS)
    conn.execute("CREATE SCHEMA IF NOT EXISTS travelcanary_marts")
    conn.execute(f"CREATE OR REPLACE TABLE {HISTORY_RELATION} ({col_defs})")


def _history_row(
    *, iso3: str, issuer: str, snapshot_date: str, ordinal: str
) -> list[str]:
    values = {
        "destination_iso2": iso3[:2],
        "destination_iso3": iso3,
        "destination_name": iso3,
        "issuing_government": issuer,
        "snapshot_date": snapshot_date,
        "native_level": ordinal,
        "native_level_label": f"level-{ordinal}",
        "normalized_ordinal": ordinal,
        "normalized_level_label": f"normalized-{ordinal}",
        "normalization_status": "mapped",
        "summary_text": "summary",
        "source_url": "https://example.test",
        "source_published_at": "2026-07-01T00:00:00Z",
        "source_ingested_at": "2026-07-01T00:00:00Z",
        "snapshot_recorded_at": "2026-07-01T00:00:00Z",
    }
    return [values[column] for column in HISTORY_COLUMNS]


def _insert_history_rows(
    conn: duckdb.DuckDBPyConnection, rows: list[list[str]]
) -> None:
    placeholders = ", ".join("?" for _ in HISTORY_COLUMNS)
    for row in rows:
        conn.execute(
            f"INSERT INTO {HISTORY_RELATION} VALUES ({placeholders})",
            row,
        )
    conn.commit()


def test_export_and_import_history_round_trip_and_existing_rows_win(tmp_path):
    conn = get_persistent_connection()
    try:
        _create_history_table(conn)
        _insert_history_rows(
            conn,
            [
                _history_row(
                    iso3="THA",
                    issuer="us_state",
                    snapshot_date="2026-07-01",
                    ordinal="2",
                ),
                _history_row(
                    iso3="THA",
                    issuer="us_state",
                    snapshot_date="2026-07-02",
                    ordinal="3",
                ),
            ],
        )
    finally:
        conn.close()

    parquet_path = tmp_path / "history.parquet"
    manifest = export_history(parquet_path)
    assert manifest["row_count"] == 2
    assert manifest["snapshot_date_min"] == "2026-07-01"
    assert manifest["snapshot_date_max"] == "2026-07-02"
    assert (tmp_path / "history.manifest.json").is_file()

    # Mutate the warehouse copy of the older day, then re-import: existing wins.
    conn = get_persistent_connection()
    try:
        conn.execute(
            f"""
            UPDATE {HISTORY_RELATION}
            SET normalized_ordinal = '4'
            WHERE snapshot_date = '2026-07-01'
            """
        )
        conn.execute(
            f"DELETE FROM {HISTORY_RELATION} WHERE snapshot_date = '2026-07-02'"
        )
        conn.commit()
    finally:
        conn.close()

    result = import_history(parquet_path)
    assert result["rows_inserted"] == 1
    assert result["policy"] == "existing_rows_win"

    conn = get_persistent_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT snapshot_date, normalized_ordinal
            FROM {HISTORY_RELATION}
            WHERE destination_iso3 = 'THA' AND issuing_government = 'us_state'
            ORDER BY snapshot_date
            """
        ).fetchall()
        assert rows == [("2026-07-01", "4"), ("2026-07-02", "3")]
    finally:
        conn.close()


def test_import_history_creates_table_when_absent(tmp_path):
    source = get_persistent_connection()
    try:
        _create_history_table(source)
        _insert_history_rows(
            source,
            [
                _history_row(
                    iso3="CHE", issuer="ca_gac", snapshot_date="2026-07-03", ordinal="1"
                )
            ],
        )
    finally:
        source.close()

    parquet_path = tmp_path / "seed.parquet"
    export_history(parquet_path)

    # Wipe the history relation.
    conn = get_persistent_connection()
    try:
        conn.execute(f"DROP TABLE IF EXISTS {HISTORY_RELATION}")
        conn.commit()
    finally:
        conn.close()

    result = import_history(parquet_path)
    assert result["rows_inserted"] == 1
    conn = get_persistent_connection()
    try:
        assert conn.execute(f"SELECT COUNT(*) FROM {HISTORY_RELATION}").fetchone() == (
            1,
        )
    finally:
        conn.close()


def test_export_history_rejects_missing_mart(tmp_path):
    with pytest.raises(HistoryTransferError, match="history mart is missing"):
        export_history(tmp_path / "missing.parquet")


def test_export_history_rejects_column_drift(tmp_path):
    conn = get_persistent_connection()
    try:
        _create_history_table(conn)
        conn.execute(f"ALTER TABLE {HISTORY_RELATION} ADD COLUMN drift_column VARCHAR")
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(HistoryTransferError, match="column contract mismatch"):
        export_history(tmp_path / "drift.parquet")


def test_import_history_rejects_missing_file(tmp_path):
    with pytest.raises(HistoryTransferError, match="not found"):
        import_history(tmp_path / "absent.parquet")


def test_import_history_rejects_parquet_column_drift(tmp_path):
    bad = tmp_path / "bad.parquet"
    duckdb.connect().execute(
        "COPY (SELECT 'x' AS drift_column) TO ? (FORMAT PARQUET)",
        [str(bad)],
    ).close()
    with pytest.raises(HistoryTransferError, match="column contract mismatch"):
        import_history(bad)


def test_import_history_surfaces_writer_lock_contention(tmp_path):
    conn = get_persistent_connection()
    try:
        _create_history_table(conn)
        _insert_history_rows(
            conn,
            [
                _history_row(
                    iso3="JPN",
                    issuer="jp_mofa",
                    snapshot_date="2026-07-04",
                    ordinal="1",
                )
            ],
        )
    finally:
        conn.close()

    parquet_path = tmp_path / "locked.parquet"
    export_history(parquet_path)

    with warehouse_writer_lock(active_duckdb_path()):
        with pytest.raises(RuntimeError, match="warehouse writer already active"):
            import_history(parquet_path)


def test_export_history_empty_table_records_null_date_bounds(tmp_path):
    conn = get_persistent_connection()
    try:
        _create_history_table(conn)
        conn.commit()
    finally:
        conn.close()

    manifest = export_history(tmp_path / "empty.parquet")
    assert manifest["row_count"] == 0
    assert manifest["snapshot_date_min"] is None
    assert manifest["snapshot_date_max"] is None


def test_import_history_rejects_existing_table_column_drift(tmp_path):
    conn = get_persistent_connection()
    try:
        _create_history_table(conn)
        _insert_history_rows(
            conn,
            [
                _history_row(
                    iso3="ITA",
                    issuer="uk_fcdo",
                    snapshot_date="2026-07-06",
                    ordinal="1",
                )
            ],
        )
    finally:
        conn.close()

    parquet_path = tmp_path / "ok.parquet"
    export_history(parquet_path)

    conn = get_persistent_connection()
    try:
        conn.execute(f"ALTER TABLE {HISTORY_RELATION} ADD COLUMN drift_column VARCHAR")
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(HistoryTransferError, match="column contract mismatch"):
        import_history(parquet_path)


def test_history_cli_success_and_failure(tmp_path, monkeypatch):
    from scripts import export_history as export_cli
    from scripts import import_history as import_cli

    conn = get_persistent_connection()
    try:
        _create_history_table(conn)
        _insert_history_rows(
            conn,
            [
                _history_row(
                    iso3="FRA", issuer="nl_mfa", snapshot_date="2026-07-05", ordinal="2"
                )
            ],
        )
    finally:
        conn.close()

    parquet_path = tmp_path / "cli-history.parquet"
    assert export_cli.main([str(parquet_path)]) == 0
    assert import_cli.main([str(parquet_path)]) == 0

    monkeypatch.setattr(
        export_cli,
        "export_history",
        lambda _path: (_ for _ in ()).throw(HistoryTransferError("nope")),
    )
    assert export_cli.main([str(tmp_path / "fail.parquet")]) == 1

    monkeypatch.setattr(
        import_cli,
        "import_history",
        lambda _path: (_ for _ in ()).throw(HistoryTransferError("nope")),
    )
    assert import_cli.main([str(parquet_path)]) == 1

    monkeypatch.setattr(
        import_cli,
        "import_history",
        lambda _path: (_ for _ in ()).throw(RuntimeError("locked")),
    )
    assert import_cli.main([str(parquet_path)]) == 1
