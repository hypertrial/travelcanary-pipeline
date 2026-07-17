import io
import zipfile
from datetime import date, datetime, timezone
from unittest.mock import patch

import duckdb
import pytest

from travelcanary_pipeline.ingestion.gdelt.events import (
    _GDELT_EVENT_COLUMNS,
    _daily_export_url,
    prune_gdelt_events,
    sync_gdelt_daily_events,
    upsert_gdelt_events,
)
from travelcanary_pipeline.ingestion.source_contracts import SourceContract
from travelcanary_pipeline.naming import SOURCE_GDELT, raw_schema


def _event_row(
    event_id: str,
    event_date: str,
    *,
    ingested_at: str = "2026-07-01T00:00:00+00:00",
) -> dict[str, object]:
    return {
        "source_run_id": "test-run",
        "global_event_id": event_id,
        "event_date": event_date,
        "action_geo_country_code": "TH",
        "event_root_code": "19",
        "quad_class": 4,
        "goldstein_scale": -5.0,
        "avg_tone": -3.0,
        "num_mentions": 1,
        "date_added": "20260701120000",
        "source_url": "http://example.com",
        "ingested_at": ingested_at,
    }


def _zip_with_tsv(line: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("20260701.export.CSV", line)
    return buf.getvalue()


def _gdelt_zip_bytes() -> bytes:
    values = [""] * len(_GDELT_EVENT_COLUMNS)
    values[_GDELT_EVENT_COLUMNS.index("GLOBALEVENTID")] = "99"
    values[_GDELT_EVENT_COLUMNS.index("SQLDATE")] = "20260701"
    values[_GDELT_EVENT_COLUMNS.index("EventRootCode")] = "19"
    values[_GDELT_EVENT_COLUMNS.index("QuadClass")] = "4"
    values[_GDELT_EVENT_COLUMNS.index("GoldsteinScale")] = "-5.0"
    values[_GDELT_EVENT_COLUMNS.index("AvgTone")] = "-3.0"
    values[_GDELT_EVENT_COLUMNS.index("NumMentions")] = "1"
    values[_GDELT_EVENT_COLUMNS.index("ActionGeo_CountryCode")] = "TH"
    values[_GDELT_EVENT_COLUMNS.index("DATEADDED")] = "20260701120000"
    values[_GDELT_EVENT_COLUMNS.index("SOURCEURL")] = "http://example.com"
    return _zip_with_tsv("\t".join(values))


def _fetch_gdelt_zip(_url: str, destination) -> None:
    destination.write(_gdelt_zip_bytes())


def test_upsert_gdelt_events_empty_rows_returns_zero():
    conn = duckdb.connect(":memory:")
    try:
        assert upsert_gdelt_events(conn, []) == 0
    finally:
        conn.close()


def test_upsert_gdelt_events_inserts_and_replaces_rows():
    conn = duckdb.connect(":memory:")
    schema = raw_schema(SOURCE_GDELT)
    try:
        inserted = upsert_gdelt_events(conn, [_event_row("1", "20260701")])
        assert inserted == 1
        assert conn.execute(f"SELECT COUNT(*) FROM {schema}.events").fetchone()[0] == 1

        replaced = upsert_gdelt_events(
            conn,
            [
                _event_row(
                    "1",
                    "20260702",
                    ingested_at="2026-07-02T00:00:00+00:00",
                )
            ],
        )
        assert replaced == 1
        row = conn.execute(
            f"SELECT event_date, ingested_at FROM {schema}.events WHERE global_event_id = '1'"
        ).fetchone()
        assert row == ("20260702", "2026-07-02T00:00:00+00:00")
        assert conn.execute(f"SELECT COUNT(*) FROM {schema}.events").fetchone()[0] == 1
    finally:
        conn.close()


def test_upsert_gdelt_events_batches_large_iterables_without_retaining_rows():
    class BatchConnection:
        def __init__(self):
            self.batch_sizes: list[int] = []

        def execute(self, _sql):
            return self

        def executemany(self, _sql, rows):
            self.batch_sizes.append(len(rows))
            return self

    conn = BatchConnection()
    rows = (_event_row(str(index), "20260701") for index in range(100_001))

    assert upsert_gdelt_events(conn, rows) == 100_001
    assert conn.batch_sizes == [5_000] * 20 + [1]
    assert max(conn.batch_sizes) == 5_000


def test_prune_gdelt_events_deletes_only_rows_before_cutoff():
    conn = duckdb.connect(":memory:")
    schema = raw_schema(SOURCE_GDELT)
    fixed_now = datetime(2026, 7, 10, tzinfo=timezone.utc)
    upsert_gdelt_events(
        conn,
        [
            _event_row("old", "20260708"),
            _event_row("boundary", "20260709"),
            _event_row("recent", "20260710"),
        ],
    )
    try:
        with patch("travelcanary_pipeline.ingestion.gdelt.events.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            deleted = prune_gdelt_events(conn, window_days=1)

        assert deleted == 1
        remaining = {
            row[0]
            for row in conn.execute(
                f"SELECT global_event_id FROM {schema}.events"
            ).fetchall()
        }
        assert remaining == {"boundary", "recent"}
    finally:
        conn.close()


def test_sync_gdelt_daily_events_upserts_and_commits_rows():
    conn = duckdb.connect(":memory:")
    schema = raw_schema(SOURCE_GDELT)
    target_date = date(2026, 7, 1)

    try:
        summary = sync_gdelt_daily_events(
            for_date=target_date,
            fetch_file=_fetch_gdelt_zip,
            conn=conn,
            enforce_contract=False,
        )
        assert summary["source"] == SOURCE_GDELT
        assert summary["url"] == _daily_export_url(target_date)
        assert summary["event_date"] == "2026-07-01"
        assert summary["rows"] == 1
        assert summary["pruned_rows"] == 0
        row = conn.execute(
            f"SELECT global_event_id, event_date FROM {schema}.events"
        ).fetchone()
        assert row == ("99", "20260701")
    finally:
        conn.close()


def test_sync_gdelt_daily_events_accepts_event_without_geography():
    conn = duckdb.connect(":memory:")
    row = _event_row("no-geo", "20260701")
    row["action_geo_country_code"] = None

    try:
        with patch(
            "travelcanary_pipeline.ingestion.gdelt.events.iter_gdelt_export_zip",
            return_value=iter([row]),
        ):
            summary = sync_gdelt_daily_events(
                for_date=date(2026, 7, 1),
                fetch_file=lambda _url, destination: destination.write(b"zip"),
                conn=conn,
                enforce_contract=False,
            )

        assert summary["rows"] == 1
        assert conn.execute(
            f"select action_geo_country_code from {raw_schema(SOURCE_GDELT)}.events"
        ).fetchone() == (None,)
    finally:
        conn.close()


def _contract(minimum_rows: int = 1) -> SourceContract:
    return SourceContract(
        source=SOURCE_GDELT,
        role="required",
        minimum_rows=minimum_rows,
        warn_drop_ratio=0.9,
        reject_drop_ratio=0.8,
        warn_after_hours=36,
        error_after_hours=72,
        minimum_canonical_ratio=0.95,
        warn_canonical_ratio=0.98,
        minimum_normalization_ratio=0.0,
    )


def test_guarded_gdelt_run_accepts_then_rejects_a_small_batch():
    conn = duckdb.connect(":memory:")
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value={SOURCE_GDELT: _contract()},
        ),
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events._load_fips_codes",
            return_value={"TH"},
        ),
    ):
        result = sync_gdelt_daily_events(
            for_date=date(2026, 7, 1),
            fetch_file=_fetch_gdelt_zip,
            conn=conn,
        )
    assert result["rows"] == 1
    assert conn.execute(
        "select status from travelcanary_ops.source_sync_runs"
    ).fetchone() == ("accepted",)

    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value={SOURCE_GDELT: _contract(2)},
        ),
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events._load_fips_codes",
            return_value={"TH"},
        ),
        pytest.raises(ValueError, match="batch rejected"),
    ):
        sync_gdelt_daily_events(
            for_date=date(2026, 7, 2),
            fetch_file=_fetch_gdelt_zip,
            conn=conn,
        )
    assert {
        row[0]
        for row in conn.execute(
            "select status from travelcanary_ops.source_sync_runs"
        ).fetchall()
    } == {"accepted", "rejected"}
    assert conn.execute(
        f"select source_run_id, event_date from {raw_schema(SOURCE_GDELT)}.events"
    ).fetchall() == [(result["source_run_id"], "20260701")]
    conn.close()


def test_guarded_gdelt_write_failure_records_error():
    conn = duckdb.connect(":memory:")
    upsert_gdelt_events(conn, [_event_row("99", "20260630")])
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value={SOURCE_GDELT: _contract()},
        ),
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events._load_fips_codes",
            return_value={"TH"},
        ),
        patch(
            "travelcanary_pipeline.ingestion.gdelt.events.prune_gdelt_events",
            side_effect=RuntimeError("disk full"),
        ),
        pytest.raises(RuntimeError, match="disk full"),
    ):
        sync_gdelt_daily_events(
            for_date=date(2026, 7, 1),
            fetch_file=_fetch_gdelt_zip,
            conn=conn,
        )
    assert conn.execute(
        "select status from travelcanary_ops.source_sync_runs"
    ).fetchone() == ("error",)
    assert conn.execute(
        f"select source_run_id, event_date from {raw_schema(SOURCE_GDELT)}.events"
    ).fetchone() == ("test-run", "20260630")
    conn.close()
