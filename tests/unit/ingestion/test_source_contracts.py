import csv
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from travelcanary_pipeline.config.settings import BASE_DIR
from travelcanary_pipeline.ingestion.common.advisory import (
    BatchDiagnostics,
    row_from_parts,
)
from travelcanary_pipeline.ingestion.common.dlt_runner import sync_advisory_rows
from travelcanary_pipeline.ingestion.source_contracts import (
    SourceContract,
    canonical_country_ratio,
    load_source_contracts,
    normalization_ratio,
)
from travelcanary_pipeline.naming import (
    ALL_ADVISORY_SOURCES,
    SOURCE_GDELT,
    SOURCE_US_STATE,
)
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection
from travelcanary_pipeline.storage.duckdb.source_runs import (
    finalize_accepted_source_run,
    previous_accepted_rows,
    record_source_run,
    record_source_run_error_if_unaccepted,
    rejection_reason,
)


@pytest.fixture
def contract() -> SourceContract:
    return SourceContract(
        source=SOURCE_US_STATE,
        role="required",
        minimum_rows=80,
        warn_drop_ratio=0.9,
        reject_drop_ratio=0.8,
        warn_after_hours=36,
        error_after_hours=72,
        minimum_canonical_ratio=0.98,
        warn_canonical_ratio=0.99,
        minimum_normalization_ratio=0.99,
    )


def _reason(
    contract: SourceContract,
    rows: int,
    *,
    previous: int | None = None,
    canonical: float = 1.0,
    normalized: float = 1.0,
) -> str | None:
    return rejection_reason(
        contract,
        fetched_rows=rows,
        previous_rows=previous,
        canonical_ratio=canonical,
        normalization_ratio=normalized,
    )


def test_contract_guard_first_healthy_warning_and_rejected_runs(contract):
    assert _reason(contract, 80) is None
    assert _reason(contract, 100, previous=100) is None
    assert _reason(contract, 85, previous=100) is None  # accepted with warning
    relative_contract = contract.__class__(**{**contract.__dict__, "minimum_rows": 1})
    assert "ratio" in _reason(relative_contract, 79, previous=100)
    assert "floor" in _reason(contract, 79)
    assert "canonical" in _reason(contract, 100, canonical=0.97)
    assert "normalization" in _reason(contract, 100, normalized=0.98)


def test_contract_seed_loads_every_public_source():
    contracts = load_source_contracts()
    assert len(contracts) == len(ALL_ADVISORY_SOURCES) + 1
    assert set(contracts) == {*ALL_ADVISORY_SOURCES, SOURCE_GDELT}
    assert contracts[SOURCE_US_STATE].required
    assert contracts[SOURCE_US_STATE].warn_canonical_ratio == 0.99
    assert contracts["gdelt"].warn_canonical_ratio == 0.98


def _contract_rows():
    with (BASE_DIR / "dbt" / "seeds" / "source_contracts.csv").open() as handle:
        return list(csv.DictReader(handle))


def _write_contracts(path, rows) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("minimum_rows", "0", "must be positive"),
        ("warn_after_hours", "72", "must be less"),
        ("warn_drop_ratio", "1.1", "between 0 and 1"),
        ("reject_drop_ratio", "0.95", "must not exceed"),
        ("minimum_canonical_ratio", "1", "must not exceed"),
        ("minimum_normalization_ratio", "bad", "invalid value"),
    ],
)
def test_contract_seed_rejects_invalid_semantics(tmp_path, field, value, message):
    rows = _contract_rows()
    rows[0][field] = value
    path = tmp_path / "source_contracts.csv"
    _write_contracts(path, rows)

    with pytest.raises(ValueError, match=message) as exc_info:
        load_source_contracts(path)
    error = str(exc_info.value)
    assert "source=us_state" in error
    assert f"field={field}" in error
    assert "value" in error


def test_contract_seed_rejects_duplicates_missing_sources_and_roles(tmp_path):
    rows = _contract_rows()
    path = tmp_path / "source_contracts.csv"

    _write_contracts(path, [*rows, rows[0]])
    with pytest.raises(ValueError, match="duplicate source"):
        load_source_contracts(path)

    _write_contracts(path, rows[:-1])
    with pytest.raises(ValueError, match="source set mismatch"):
        load_source_contracts(path)

    rows[0]["role"] = "optional"
    _write_contracts(path, rows)
    with pytest.raises(ValueError, match="field=role"):
        load_source_contracts(path)


def test_country_and_row_weighted_normalization_coverage():
    rows = [
        {"destination_iso2": "TH", "native_level": "1"},
        {"destination_iso3": "CHE", "native_level": "1"},
        {"destination_iso2": "not-iso", "native_level": "future-value"},
        {"destination_iso2": "US", "native_level": "future-value"},
    ]
    assert canonical_country_ratio(rows) == 0.75
    assert canonical_country_ratio([]) == 0.0
    assert normalization_ratio(SOURCE_US_STATE, rows) == 0.5


def test_source_run_ledger_only_returns_accepted(contract):
    conn = get_persistent_connection()
    try:
        record_source_run(
            conn,
            source_run_id="rejected",
            source=SOURCE_US_STATE,
            contract=contract,
            status="rejected",
            started_at=datetime.now(timezone.utc),
            fetched_rows=70,
            previous_rows=None,
            canonical_ratio=1.0,
            normalization_ratio=1.0,
        )
        assert previous_accepted_rows(conn, SOURCE_US_STATE) is None
        record_source_run(
            conn,
            source_run_id="accepted",
            source=SOURCE_US_STATE,
            contract=contract,
            status="accepted",
            started_at=datetime.now(timezone.utc),
            fetched_rows=100,
            previous_rows=None,
            canonical_ratio=1.0,
            normalization_ratio=1.0,
        )
        assert previous_accepted_rows(conn, SOURCE_US_STATE) == 100
    finally:
        conn.close()


def test_write_failure_is_recorded_but_not_accepted(contract):
    row = row_from_parts(
        source=SOURCE_US_STATE,
        destination_native_id="TH",
        destination_iso2="TH",
        native_level="1",
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    permissive = {
        SOURCE_US_STATE: contract.__class__(**{**contract.__dict__, "minimum_rows": 1})
    }
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value=permissive,
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load",
            side_effect=RuntimeError("write failed"),
        ),
        pytest.raises(RuntimeError, match="write failed"),
    ):
        sync_advisory_rows(source=SOURCE_US_STATE, rows=[row])

    conn = get_persistent_connection()
    try:
        statuses = conn.execute(
            "select status from travelcanary_ops.source_sync_runs"
        ).fetchall()
        assert statuses == [("error",)]
        assert previous_accepted_rows(conn, SOURCE_US_STATE) is None
    finally:
        conn.close()


def test_shared_advisory_guard_accepts_and_rejects_before_write(contract):
    row = row_from_parts(
        source=SOURCE_US_STATE,
        destination_native_id="TH",
        destination_iso2="TH",
        native_level="1",
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    permissive = {
        SOURCE_US_STATE: contract.__class__(**{**contract.__dict__, "minimum_rows": 1})
    }
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value=permissive,
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
        ) as merge,
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.finalize_accepted_source_run"
        ),
    ):
        result = sync_advisory_rows(source=SOURCE_US_STATE, rows=[row])
    assert result["rows"] == 1
    merge.assert_called_once()

    strict = {
        SOURCE_US_STATE: contract.__class__(**{**contract.__dict__, "minimum_rows": 2})
    }
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value=strict,
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
        ) as merge,
        pytest.raises(ValueError, match="batch rejected"),
    ):
        sync_advisory_rows(source=SOURCE_US_STATE, rows=[row])
    merge.assert_not_called()


def test_official_append_preserves_last_acceptance_until_successor_finalizes(contract):
    permissive = {
        SOURCE_US_STATE: contract.__class__(**{**contract.__dict__, "minimum_rows": 1})
    }

    def advisory(level: str):
        return row_from_parts(
            source=SOURCE_US_STATE,
            destination_native_id="TH",
            destination_iso2="TH",
            destination_iso3="THA",
            native_level=level,
            ingested_at=f"2026-07-0{level}T00:00:00+00:00",
        )

    with patch(
        "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
        return_value=permissive,
    ):
        accepted_a = sync_advisory_rows(source=SOURCE_US_STATE, rows=[advisory("1")])
        with (
            patch(
                "travelcanary_pipeline.ingestion.common.dlt_runner.finalize_accepted_source_run",
                side_effect=RuntimeError("finalization failed"),
            ),
            pytest.raises(RuntimeError, match="finalization failed"),
        ):
            sync_advisory_rows(source=SOURCE_US_STATE, rows=[advisory("2")])

        conn = get_persistent_connection()
        try:
            visible = conn.execute(
                """
                select a.source_run_id, a.native_level
                from us_state_raw.advisories as a
                inner join (
                    select source_run_id
                    from travelcanary_ops.source_sync_runs
                    where source = ? and status = 'accepted'
                    order by finished_at desc
                    limit 1
                ) as accepted using (source_run_id)
                """,
                [SOURCE_US_STATE],
            ).fetchall()
            assert visible == [(accepted_a["source_run_id"], "1")]
            assert conn.execute(
                "select count(*) from us_state_raw.advisories"
            ).fetchone() == (2,)
        finally:
            conn.close()

        accepted_c = sync_advisory_rows(source=SOURCE_US_STATE, rows=[advisory("3")])

    conn = get_persistent_connection()
    try:
        assert conn.execute(
            "select source_run_id, native_level from us_state_raw.advisories"
        ).fetchall() == [(accepted_c["source_run_id"], "3")]
    finally:
        conn.close()


def test_duplicate_official_keys_are_rejected_before_landing(contract):
    row = row_from_parts(
        source=SOURCE_US_STATE,
        destination_native_id="TH",
        destination_iso2="TH",
        native_level="1",
    )
    permissive = {
        SOURCE_US_STATE: contract.__class__(**{**contract.__dict__, "minimum_rows": 1})
    }
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value=permissive,
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
        ) as load,
        pytest.raises(ValueError, match="duplicate advisory_id"),
    ):
        sync_advisory_rows(source=SOURCE_US_STATE, rows=[row, row])
    load.assert_not_called()


def test_duplicate_official_keys_are_rejected_without_contract_guard():
    row = row_from_parts(
        source=SOURCE_US_STATE,
        destination_native_id="TH",
        destination_iso2="TH",
        native_level="1",
    )
    with (
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
        ) as load,
        pytest.raises(ValueError, match="duplicate advisory_id"),
    ):
        sync_advisory_rows(
            source=SOURCE_US_STATE,
            rows=[row, row],
            enforce_contract=False,
        )
    load.assert_not_called()


def test_blocking_parse_loss_is_rejected_and_benign_skips_are_summarized(contract):
    row = row_from_parts(
        source=SOURCE_US_STATE,
        destination_native_id="TH",
        destination_iso2="TH",
        native_level="1",
    )
    permissive = {
        SOURCE_US_STATE: contract.__class__(**{**contract.__dict__, "minimum_rows": 1})
    }
    blocking = BatchDiagnostics()
    blocking.observe()
    blocking.skip("non-object advisory")
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value=permissive,
        ),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
        ) as load,
        pytest.raises(ValueError, match="malformed source records"),
    ):
        sync_advisory_rows(
            source=SOURCE_US_STATE,
            rows=[row],
            diagnostics=blocking,
        )
    load.assert_not_called()

    benign = BatchDiagnostics()
    benign.observe()
    benign.skip("withdrawn advisory (HTTP 404)", blocking=False)
    with (
        patch(
            "travelcanary_pipeline.ingestion.source_contracts.load_source_contracts",
            return_value=permissive,
        ),
        patch("travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"),
        patch(
            "travelcanary_pipeline.ingestion.common.dlt_runner.finalize_accepted_source_run"
        ) as finalize,
    ):
        result = sync_advisory_rows(
            source=SOURCE_US_STATE,
            rows=[row],
            diagnostics=benign,
        )

    assert result["skipped_rows"] == 1
    assert result["blocking_skipped_rows"] == 0
    assert "withdrawn advisory" in finalize.call_args.kwargs["message"]


def test_accepted_finalization_rolls_back_ledger_when_pruning_fails(contract):
    conn = get_persistent_connection()
    try:
        with pytest.raises(Exception, match="missing_raw"):
            finalize_accepted_source_run(
                conn,
                raw_relation="missing_raw.advisories",
                source_run_id="failed-finalization",
                source=SOURCE_US_STATE,
                contract=contract,
                started_at=datetime.now(timezone.utc),
                fetched_rows=100,
                previous_rows=None,
                canonical_ratio=1.0,
                normalization_ratio=1.0,
            )
        assert conn.execute(
            "select count(*) from travelcanary_ops.source_sync_runs"
        ).fetchone() == (0,)
    finally:
        conn.close()


def test_error_recording_never_overwrites_an_accepted_run(contract):
    conn = get_persistent_connection()
    started_at = datetime.now(timezone.utc)
    try:
        record_source_run(
            conn,
            source_run_id="accepted",
            source=SOURCE_US_STATE,
            contract=contract,
            status="accepted",
            started_at=started_at,
            fetched_rows=100,
            previous_rows=None,
            canonical_ratio=1.0,
            normalization_ratio=1.0,
        )
        conn.commit()

        assert not record_source_run_error_if_unaccepted(
            conn,
            source_run_id="accepted",
            source=SOURCE_US_STATE,
            contract=contract,
            started_at=started_at,
            fetched_rows=100,
            previous_rows=None,
            canonical_ratio=1.0,
            normalization_ratio=1.0,
            message="late failure",
        )
        assert conn.execute(
            "select status from travelcanary_ops.source_sync_runs"
        ).fetchone() == ("accepted",)
    finally:
        conn.close()
