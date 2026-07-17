"""Append-only source run accounting and batch guardrails."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb

from travelcanary_pipeline.ingestion.source_contracts import SourceContract


@dataclass(frozen=True)
class SourceRunEvaluation:
    previous_rows: int | None
    rejection_reason: str | None


def ensure_source_runs_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS travelcanary_ops")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS travelcanary_ops.source_sync_runs (
            source_run_id VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            role VARCHAR NOT NULL,
            status VARCHAR NOT NULL CHECK (
                status IN ('accepted', 'rejected', 'unavailable', 'error')
            ),
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP NOT NULL,
            fetched_rows BIGINT NOT NULL,
            previous_accepted_rows BIGINT,
            minimum_rows BIGINT NOT NULL,
            relative_ratio DOUBLE,
            canonical_ratio DOUBLE,
            normalization_ratio DOUBLE,
            message VARCHAR
        )
        """
    )


def previous_accepted_rows(conn: duckdb.DuckDBPyConnection, source: str) -> int | None:
    ensure_source_runs_table(conn)
    row = conn.execute(
        """
        SELECT fetched_rows
        FROM travelcanary_ops.source_sync_runs
        WHERE source = ? AND status = 'accepted'
        ORDER BY finished_at DESC
        LIMIT 1
        """,
        [source],
    ).fetchone()
    return int(row[0]) if row else None


def rejection_reason(
    contract: SourceContract,
    *,
    fetched_rows: int,
    previous_rows: int | None,
    canonical_ratio: float,
    normalization_ratio: float,
) -> str | None:
    if fetched_rows < contract.minimum_rows:
        return f"row count {fetched_rows} below floor {contract.minimum_rows}"
    if previous_rows and fetched_rows / previous_rows < contract.reject_drop_ratio:
        return (
            f"row count ratio {fetched_rows / previous_rows:.3f} below "
            f"{contract.reject_drop_ratio:.3f}"
        )
    if canonical_ratio < contract.minimum_canonical_ratio:
        return (
            f"canonical ratio {canonical_ratio:.3f} below "
            f"{contract.minimum_canonical_ratio:.3f}"
        )
    if normalization_ratio < contract.minimum_normalization_ratio:
        return (
            f"normalization ratio {normalization_ratio:.3f} below "
            f"{contract.minimum_normalization_ratio:.3f}"
        )
    return None


def evaluate_source_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    source: str,
    contract: SourceContract,
    fetched_rows: int,
    canonical_ratio: float,
    normalization_ratio: float,
) -> SourceRunEvaluation:
    previous_rows = previous_accepted_rows(conn, source)
    return evaluate_source_run_metrics(
        contract=contract,
        fetched_rows=fetched_rows,
        previous_rows=previous_rows,
        canonical_ratio=canonical_ratio,
        normalization_ratio=normalization_ratio,
    )


def evaluate_source_run_metrics(
    *,
    contract: SourceContract,
    fetched_rows: int,
    previous_rows: int | None,
    canonical_ratio: float,
    normalization_ratio: float,
) -> SourceRunEvaluation:
    return SourceRunEvaluation(
        previous_rows=previous_rows,
        rejection_reason=rejection_reason(
            contract,
            fetched_rows=fetched_rows,
            previous_rows=previous_rows,
            canonical_ratio=canonical_ratio,
            normalization_ratio=normalization_ratio,
        ),
    )


def record_source_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_run_id: str,
    source: str,
    contract: SourceContract,
    status: str,
    started_at: datetime,
    fetched_rows: int,
    previous_rows: int | None,
    canonical_ratio: float | None,
    normalization_ratio: float | None,
    message: str | None = None,
) -> None:
    ensure_source_runs_table(conn)
    relative_ratio = (
        fetched_rows / previous_rows if previous_rows and fetched_rows else None
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO travelcanary_ops.source_sync_runs VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            source_run_id,
            source,
            contract.role,
            status,
            started_at,
            datetime.now(timezone.utc),
            fetched_rows,
            previous_rows,
            contract.minimum_rows,
            relative_ratio,
            canonical_ratio,
            normalization_ratio,
            message,
        ],
    )


def record_source_run_status(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_run_id: str,
    source: str,
    contract: SourceContract,
    status: str,
    started_at: datetime,
    fetched_rows: int,
    previous_rows: int | None,
    canonical_ratio: float | None,
    normalization_ratio: float | None,
    message: str | None = None,
) -> None:
    record_source_run(
        conn,
        source_run_id=source_run_id,
        source=source,
        contract=contract,
        status=status,
        started_at=started_at,
        fetched_rows=fetched_rows,
        previous_rows=previous_rows,
        canonical_ratio=canonical_ratio,
        normalization_ratio=normalization_ratio,
        message=message[:1000] if message is not None else None,
    )


def finalize_accepted_source_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    raw_relation: str,
    source_run_id: str,
    source: str,
    contract: SourceContract,
    started_at: datetime,
    fetched_rows: int,
    previous_rows: int | None,
    canonical_ratio: float,
    normalization_ratio: float,
    message: str | None = None,
) -> None:
    conn.execute("BEGIN TRANSACTION")
    try:
        record_source_run_status(
            conn,
            source_run_id=source_run_id,
            source=source,
            contract=contract,
            status="accepted",
            started_at=started_at,
            fetched_rows=fetched_rows,
            previous_rows=previous_rows,
            canonical_ratio=canonical_ratio,
            normalization_ratio=normalization_ratio,
            message=message,
        )
        conn.execute(
            f"DELETE FROM {raw_relation} WHERE source_run_id IS DISTINCT FROM ?",
            [source_run_id],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def record_source_run_error_if_unaccepted(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_run_id: str,
    source: str,
    contract: SourceContract,
    started_at: datetime,
    fetched_rows: int,
    previous_rows: int | None,
    canonical_ratio: float | None,
    normalization_ratio: float | None,
    message: str,
) -> bool:
    ensure_source_runs_table(conn)
    row = conn.execute(
        "SELECT status FROM travelcanary_ops.source_sync_runs WHERE source_run_id = ?",
        [source_run_id],
    ).fetchone()
    if row and row[0] == "accepted":
        return False
    record_source_run_status(
        conn,
        source_run_id=source_run_id,
        source=source,
        contract=contract,
        status="error",
        started_at=started_at,
        fetched_rows=fetched_rows,
        previous_rows=previous_rows,
        canonical_ratio=canonical_ratio,
        normalization_ratio=normalization_ratio,
        message=message,
    )
    return True


__all__ = [
    "SourceRunEvaluation",
    "evaluate_source_run",
    "evaluate_source_run_metrics",
    "ensure_source_runs_table",
    "finalize_accepted_source_run",
    "previous_accepted_rows",
    "record_source_run",
    "record_source_run_error_if_unaccepted",
    "record_source_run_status",
    "rejection_reason",
]
