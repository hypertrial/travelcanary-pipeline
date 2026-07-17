from unittest.mock import patch

import pytest

from travelcanary_pipeline.ingestion.common.advisory import row_from_parts
from travelcanary_pipeline.ingestion.common.dlt_runner import (
    _PIPELINES,
    _pipeline,
    run_advisory_load,
    run_resource_load,
    sync_advisory_rows,
)
from travelcanary_pipeline.naming import SOURCE_US_STATE
from travelcanary_pipeline.storage.duckdb.source_runs import SourceRunEvaluation


def test_sync_advisory_rows_loads_and_returns_summary():
    rows = [
        row_from_parts(
            source=SOURCE_US_STATE,
            destination_native_id="TH",
            destination_iso2="TH",
            destination_iso3="THA",
            ingested_at="2026-07-01T00:00:00+00:00",
        )
    ]
    with patch(
        "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
    ) as mock_load:
        summary = sync_advisory_rows(
            source=SOURCE_US_STATE,
            rows=rows,
            enforce_contract=False,
            url="https://example.com",
        )

    mock_load.assert_called_once()
    assert summary["source"] == SOURCE_US_STATE
    assert summary["rows"] == 1
    assert summary["canonical_ratio"] == 1.0
    assert summary["url"] == "https://example.com"


def test_pipeline_cache_and_pending_packages_are_reused():
    _PIPELINES.clear()
    pipe = type(
        "Pipe",
        (),
        {
            "has_pending_data": True,
            "drop_pending_packages": lambda self: setattr(self, "dropped", True),
            "run": lambda self, source: setattr(self, "ran", source),
        },
    )()

    with (
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.dlt.pipeline",
            return_value=pipe,
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.connection.ensure_duck_db",
            return_value=None,
        ),
        patch(
            "travelcanary_pipeline.storage.duckdb.connection.active_duckdb_path",
            return_value="/tmp/test.duckdb",
        ),
    ):
        assert _pipeline("schema") is pipe
        assert _pipeline("schema") is pipe
        run_advisory_load(schema="schema", source_name=SOURCE_US_STATE, rows=[{}])
        run_resource_load(schema="schema", source=object())
        pipe.has_pending_data = False
        run_resource_load(schema="schema", source=object())

    assert pipe.dropped is True
    assert hasattr(pipe, "ran")


def test_run_advisory_load_rejects_empty_rows():
    with pytest.raises(ValueError, match="must not be empty"):
        run_advisory_load(schema="schema", source_name=SOURCE_US_STATE, rows=[])


def test_sync_advisory_rows_records_error_when_load_fails():
    rows = [
        row_from_parts(
            source=SOURCE_US_STATE,
            destination_native_id="TH",
            destination_iso2="TH",
            destination_iso3="THA",
            ingested_at="2026-07-01T00:00:00+00:00",
        )
    ]

    with (
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.evaluate_source_run",
            return_value=SourceRunEvaluation(previous_rows=None, rejection_reason=None),
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load",
            side_effect=RuntimeError("load failed"),
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.record_source_run_error_if_unaccepted",
            return_value=True,
        ) as record,
        pytest.raises(RuntimeError, match="load failed"),
    ):
        sync_advisory_rows(source=SOURCE_US_STATE, rows=rows)

    record.assert_called_once()


def test_sync_advisory_rows_reraises_load_failure_without_contract_recording():
    rows = [
        row_from_parts(
            source=SOURCE_US_STATE,
            destination_native_id="TH",
            destination_iso2="TH",
            destination_iso3="THA",
            ingested_at="2026-07-01T00:00:00+00:00",
        )
    ]

    with (
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load",
            side_effect=RuntimeError("load failed"),
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.record_source_run_status"
        ) as record,
        pytest.raises(RuntimeError, match="load failed"),
    ):
        sync_advisory_rows(source=SOURCE_US_STATE, rows=rows, enforce_contract=False)

    record.assert_not_called()
