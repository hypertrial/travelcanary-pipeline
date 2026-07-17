"""dlt landing helpers."""

from __future__ import annotations

import os
from collections.abc import Iterable
from contextlib import suppress
from datetime import datetime, timezone
from hashlib import blake2b
from typing import Any
from uuid import uuid4

import dlt

from travelcanary_pipeline.ingestion.common.advisory import (
    ADVISORY_COLUMNS,
    AdvisoryRow,
    BatchDiagnostics,
    rows_to_dicts,
)
from travelcanary_pipeline.naming import raw_schema
from travelcanary_pipeline.storage.duckdb import connection as duckdb_connection
from travelcanary_pipeline.storage.duckdb.source_runs import (
    evaluate_source_run,
    finalize_accepted_source_run,
    record_source_run_error_if_unaccepted,
    record_source_run_status,
)

DLT_STRICT_SCHEMA_CONTRACT = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}

_PIPELINES: dict[tuple[str, str], dlt.Pipeline] = {}
_BATCH_PIPELINE_RUN_ID = f"{os.getpid():x}"


def reset_dlt_pipelines() -> None:
    _PIPELINES.clear()


def _pipeline(dataset_name: str) -> dlt.Pipeline:
    duckdb_connection.ensure_duck_db()
    db_path = str(duckdb_connection.active_duckdb_path())
    key = (dataset_name, db_path)
    if key not in _PIPELINES:
        path_hash = blake2b(db_path.encode("utf-8"), digest_size=12).hexdigest()
        _PIPELINES[key] = dlt.pipeline(
            pipeline_name=f"travelcanary_{dataset_name}_{path_hash}_{_BATCH_PIPELINE_RUN_ID}",
            destination=dlt.destinations.duckdb(credentials=db_path),
            dataset_name=dataset_name,
        )
    return _PIPELINES[key]


def advisory_source(
    *,
    source_name: str,
    rows: Iterable[dict[str, Any]] = (),
):
    @dlt.source(name=source_name)
    def _source():
        @dlt.resource(
            name="advisories",
            primary_key="advisory_id",
            write_disposition="append",
            schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
            columns=ADVISORY_COLUMNS,
        )
        def advisories():
            yield from rows

        return advisories

    return _source()


def run_advisory_load(
    *, schema: str, source_name: str, rows: list[dict[str, Any]]
) -> None:
    if not rows:
        raise ValueError("rows must not be empty")
    pipe = _pipeline(schema)
    if pipe.has_pending_data:
        pipe.drop_pending_packages()
    pipe.run(advisory_source(source_name=source_name, rows=rows))


def sync_advisory_rows(
    *,
    source: str,
    rows: list[AdvisoryRow],
    diagnostics: BatchDiagnostics | None = None,
    enforce_contract: bool = True,
    **extra_summary: object,
) -> dict[str, object]:
    from travelcanary_pipeline.storage.duckdb.writer_lock import (
        warehouse_writer_lock,
    )

    with warehouse_writer_lock(duckdb_connection.active_duckdb_path()):
        return _sync_advisory_rows_unlocked(
            source=source,
            rows=rows,
            diagnostics=diagnostics,
            enforce_contract=enforce_contract,
            **extra_summary,
        )


def _sync_advisory_rows_unlocked(
    *,
    source: str,
    rows: list[AdvisoryRow],
    diagnostics: BatchDiagnostics | None = None,
    enforce_contract: bool = True,
    **extra_summary: object,
) -> dict[str, object]:
    from travelcanary_pipeline.ingestion.source_contracts import (
        canonical_country_ratio,
        duplicate_key_reason,
        load_source_contracts,
        normalization_ratio,
    )

    source_run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    diagnostics = diagnostics or BatchDiagnostics(discovered_rows=len(rows))
    dictionaries = rows_to_dicts(rows)
    for row in dictionaries:
        row["source_run_id"] = source_run_id
    contract = load_source_contracts()[source]
    canonical_ratio = canonical_country_ratio(dictionaries)
    normalized_ratio = normalization_ratio(source, dictionaries)
    duplicate_reason = duplicate_key_reason(dictionaries, "advisory_id")
    previous_rows = None
    blocking_reason = (
        f"malformed source records: {diagnostics.message()}"
        if diagnostics.blocking_skipped_rows
        else None
    )
    if enforce_contract:
        conn = duckdb_connection.get_persistent_connection()
        try:
            evaluation = evaluate_source_run(
                conn,
                source=source,
                contract=contract,
                fetched_rows=len(dictionaries),
                canonical_ratio=canonical_ratio,
                normalization_ratio=normalized_ratio,
            )
            previous_rows = evaluation.previous_rows
            rejection_reason = (
                blocking_reason or duplicate_reason or evaluation.rejection_reason
            )
            if rejection_reason:
                record_source_run_status(
                    conn,
                    source_run_id=source_run_id,
                    source=source,
                    contract=contract,
                    status="rejected",
                    started_at=started_at,
                    fetched_rows=len(dictionaries),
                    previous_rows=previous_rows,
                    canonical_ratio=canonical_ratio,
                    normalization_ratio=normalized_ratio,
                    message=rejection_reason,
                )
                conn.commit()
                raise ValueError(f"{source} batch rejected: {rejection_reason}")
        finally:
            conn.close()
    elif blocking_reason or duplicate_reason:
        raise ValueError(
            f"{source} batch rejected: {blocking_reason or duplicate_reason}"
        )
    try:
        run_advisory_load(
            schema=raw_schema(source), source_name=source, rows=dictionaries
        )
    except Exception as exc:
        if enforce_contract:
            conn = duckdb_connection.get_persistent_connection()
            try:
                record_source_run_error_if_unaccepted(
                    conn,
                    source_run_id=source_run_id,
                    source=source,
                    contract=contract,
                    started_at=started_at,
                    fetched_rows=len(dictionaries),
                    previous_rows=previous_rows,
                    canonical_ratio=canonical_ratio,
                    normalization_ratio=normalized_ratio,
                    message=str(exc),
                )
                conn.commit()
            finally:
                conn.close()
        raise
    if enforce_contract:
        conn = duckdb_connection.get_persistent_connection()
        try:
            finalize_accepted_source_run(
                conn,
                raw_relation=f"{raw_schema(source)}.advisories",
                source_run_id=source_run_id,
                source=source,
                contract=contract,
                started_at=started_at,
                fetched_rows=len(dictionaries),
                previous_rows=previous_rows,
                canonical_ratio=canonical_ratio,
                normalization_ratio=normalized_ratio,
                message=diagnostics.message(),
            )
        except Exception as exc:
            with suppress(Exception):
                record_source_run_error_if_unaccepted(
                    conn,
                    source_run_id=source_run_id,
                    source=source,
                    contract=contract,
                    started_at=started_at,
                    fetched_rows=len(dictionaries),
                    previous_rows=previous_rows,
                    canonical_ratio=canonical_ratio,
                    normalization_ratio=normalized_ratio,
                    message=str(exc),
                )
                conn.commit()
            raise
        finally:
            conn.close()
    return {
        "source": source,
        "source_run_id": source_run_id,
        "rows": len(rows),
        "canonical_ratio": canonical_ratio,
        "normalization_ratio": normalized_ratio,
        **extra_summary,
        **diagnostics.summary(),
    }


def run_resource_load(*, schema: str, source: Any) -> None:
    pipe = _pipeline(schema)
    if pipe.has_pending_data:
        pipe.drop_pending_packages()
    pipe.run(source)


__all__ = [
    "DLT_STRICT_SCHEMA_CONTRACT",
    "advisory_source",
    "reset_dlt_pipelines",
    "run_advisory_load",
    "run_resource_load",
    "sync_advisory_rows",
]
