"""Read-only live audit engine for every TravelCanary source contract."""

from __future__ import annotations

import math
import tempfile
from collections.abc import Callable, Iterable
from datetime import date, datetime, timezone
from functools import partial
from pathlib import Path

import duckdb

from travelcanary_pipeline.ingestion.ca_gac.advisories import (
    CA_INDEX_URL,
    fetch_ca_advisories,
)
from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.ingestion.gdelt.events import (
    _daily_export_url,
    _load_fips_codes,
    iter_gdelt_export_zip,
)
from travelcanary_pipeline.ingestion.jp_mofa.advisories import (
    JP_COUNTRY_XML_BASE,
    fetch_jp_advisories,
)
from travelcanary_pipeline.ingestion.nl_mfa.advisories import (
    NL_TRAVELADVICE_INDEX_URL,
    fetch_nl_advisories,
)
from travelcanary_pipeline.ingestion.source_contracts import (
    SourceContract,
    canonical_country_ratio,
    load_source_contracts,
    normalization_ratio,
)
from travelcanary_pipeline.ingestion.uk_fcdo.advisories import (
    UK_CONTENT_API_BASE,
    fetch_uk_advisories,
)
from travelcanary_pipeline.ingestion.us_state.advisories import (
    US_TRAVEL_ADVISORIES_URL,
    fetch_us_advisories,
)
from travelcanary_pipeline.naming import (
    SOURCE_CA_GAC,
    SOURCE_GDELT,
    SOURCE_JP_MOFA,
    SOURCE_NL_MFA,
    SOURCE_UK_FCDO,
    SOURCE_US_STATE,
)
from travelcanary_pipeline.resources.http import download_to_file
from travelcanary_pipeline.storage.duckdb.source_runs import evaluate_source_run_metrics

ADVISORY_SOURCES: tuple[
    tuple[str, Callable[..., list[dict[str, object]]], str], ...
] = (
    (SOURCE_US_STATE, fetch_us_advisories, US_TRAVEL_ADVISORIES_URL),
    (SOURCE_CA_GAC, fetch_ca_advisories, CA_INDEX_URL),
    (
        SOURCE_UK_FCDO,
        lambda *, diagnostics=None: fetch_uk_advisories(
            discover_all=True, diagnostics=diagnostics
        ),
        UK_CONTENT_API_BASE,
    ),
    (SOURCE_NL_MFA, fetch_nl_advisories, NL_TRAVELADVICE_INDEX_URL),
    (SOURCE_JP_MOFA, fetch_jp_advisories, JP_COUNTRY_XML_BASE),
)

AUDIT_SOURCE_ORDER = tuple(source for source, _, _ in ADVISORY_SOURCES) + (
    SOURCE_GDELT,
)

PREVIOUS_CONTEXT_UNAVAILABLE = "unavailable"


def _relative_ratio(rows: int, previous_rows: int | None) -> float | None:
    return round(rows / previous_rows, 4) if previous_rows else None


def _concise_previous_context_reason(reason: str) -> str:
    lowered = reason.lower()
    if "source_sync_runs" in lowered:
        return "source_sync_runs not found in warehouse"
    if "database is locked" in lowered or "could not set lock" in lowered:
        return "warehouse is locked"
    return " ".join(reason.split())[:300]


def _audit_advisory(
    source: str,
    rows: list[dict[str, object]],
    *,
    previous_rows: int | None = None,
    diagnostics: BatchDiagnostics | None = None,
) -> dict[str, object]:
    diagnostics = diagnostics or BatchDiagnostics(discovered_rows=len(rows))
    if not diagnostics.discovered_rows and rows:
        diagnostics.discovered_rows = len(rows)
    contract = load_source_contracts()[source]
    canonical_ratio = canonical_country_ratio(rows)
    normalized_ratio = normalization_ratio(source, rows)
    evaluation = evaluate_source_run_metrics(
        contract=contract,
        fetched_rows=len(rows),
        previous_rows=previous_rows,
        canonical_ratio=canonical_ratio,
        normalization_ratio=normalized_ratio,
    )
    rejection_reason = (
        f"malformed source records: {diagnostics.message()}"
        if diagnostics.blocking_skipped_rows
        else evaluation.rejection_reason
    )
    return {
        "source": source,
        "role": contract.role,
        "status": "accepted" if rejection_reason is None else "rejected",
        "rows": len(rows),
        "minimum_rows": contract.minimum_rows,
        "previous_accepted_rows": previous_rows,
        "relative_ratio": _relative_ratio(len(rows), previous_rows),
        "canonical_ratio": round(canonical_ratio, 4),
        "normalization_ratio": round(normalized_ratio, 4),
        "reason": rejection_reason,
        **diagnostics.summary(),
    }


def _audit_gdelt(
    target_date: date,
    *,
    previous_rows: int | None = None,
) -> dict[str, object]:
    contract = load_source_contracts()[SOURCE_GDELT]
    url = _daily_export_url(target_date)
    mapped_codes = _load_fips_codes()
    rows = 0
    geographic_rows = 0
    mapped_rows = 0
    diagnostics = BatchDiagnostics()
    with tempfile.TemporaryFile() as zip_file:
        download_to_file(url, zip_file)
        zip_file.seek(0)
        for row in iter_gdelt_export_zip(
            zip_file,
            ingested_at=datetime.now(timezone.utc).isoformat(),
            diagnostics=diagnostics,
        ):
            rows += 1
            country_code = row["action_geo_country_code"]
            if country_code:
                geographic_rows += 1
                if country_code in mapped_codes:
                    mapped_rows += 1
    mapped_ratio = mapped_rows / geographic_rows if geographic_rows else 0.0
    if not diagnostics.discovered_rows and rows:
        diagnostics.discovered_rows = rows
    evaluation = evaluate_source_run_metrics(
        contract=contract,
        fetched_rows=rows,
        previous_rows=previous_rows,
        canonical_ratio=mapped_ratio,
        normalization_ratio=1.0,
    )
    rejection_reason = (
        f"malformed source records: {diagnostics.message()}"
        if diagnostics.blocking_skipped_rows
        else evaluation.rejection_reason
    )
    return {
        "source": SOURCE_GDELT,
        "role": contract.role,
        "status": "accepted" if rejection_reason is None else "rejected",
        "rows": rows,
        "minimum_rows": contract.minimum_rows,
        "previous_accepted_rows": previous_rows,
        "relative_ratio": _relative_ratio(rows, previous_rows),
        "mapped_geography_ratio": round(mapped_ratio, 4),
        "event_date": target_date.isoformat(),
        "reason": rejection_reason,
        **diagnostics.summary(),
    }


def _audit_advisory_fetcher(
    source: str,
    fetcher: Callable[..., list[dict[str, object]]],
    *,
    previous_rows: int | None = None,
) -> dict[str, object]:
    diagnostics = BatchDiagnostics()
    return _audit_advisory(
        source,
        fetcher(diagnostics=diagnostics),
        previous_rows=previous_rows,
        diagnostics=diagnostics,
    )


def _error_result(
    source: str, contract: SourceContract, exc: Exception
) -> dict[str, object]:
    return {
        "source": source,
        "role": contract.role,
        "status": "error",
        "rows": 0,
        "minimum_rows": contract.minimum_rows,
        "previous_accepted_rows": None,
        "relative_ratio": None,
        "reason": str(exc)[:1000],
        **BatchDiagnostics().summary(),
    }


def _source_url(source: str, target_date: date) -> str:
    if source == SOURCE_GDELT:
        return _daily_export_url(target_date)
    return dict((source, url) for source, _, url in ADVISORY_SOURCES)[source]


def select_sources(
    requested: Iterable[str] | None,
    contracts: dict[str, SourceContract] | None = None,
) -> list[str]:
    contracts = contracts or load_source_contracts()
    implemented = [source for source in AUDIT_SOURCE_ORDER if source in contracts]
    if not requested:
        return implemented
    requested_set = set(requested)
    invalid = sorted(requested_set - set(contracts))
    if invalid:
        valid = ", ".join(implemented)
        raise ValueError(f"unknown source(s): {', '.join(invalid)}; valid: {valid}")
    not_implemented = sorted(requested_set - set(implemented))
    if not_implemented:
        raise ValueError(
            f"source(s) have no live auditor: {', '.join(not_implemented)}"
        )
    return [source for source in implemented if source in requested_set]


def _previous_context_unavailable(reason: str) -> dict[str, object]:
    return {
        "previous_accepted_rows": None,
        "relative_ratio": None,
        "previous_accepted_finished_at": None,
        "previous_context_status": PREVIOUS_CONTEXT_UNAVAILABLE,
        "previous_context_reason": _concise_previous_context_reason(reason),
    }


def _format_duckdb_value(value: object) -> object:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _load_previous_context(
    warehouse: str | None,
    sources: Iterable[str],
) -> dict[str, dict[str, object]]:
    selected = list(sources)
    if warehouse is None:
        return {}
    if not selected:
        return {}
    warehouse_path = Path(warehouse).expanduser()
    if not warehouse_path.is_file():
        reason = f"warehouse not found: {warehouse_path}"
        return {source: _previous_context_unavailable(reason) for source in selected}

    default_context = {
        source: {
            "previous_accepted_rows": None,
            "relative_ratio": None,
            "previous_accepted_finished_at": None,
            "previous_context_status": "missing",
            "previous_context_reason": None,
        }
        for source in selected
    }
    placeholders = ", ".join("?" for _ in selected)
    try:
        conn = duckdb.connect(str(warehouse_path), read_only=True)
        try:
            rows = conn.execute(
                f"""
                SELECT source, fetched_rows, finished_at
                FROM (
                    SELECT
                        source,
                        fetched_rows,
                        finished_at,
                        row_number() OVER (
                            PARTITION BY source ORDER BY finished_at DESC
                        ) AS row_number
                    FROM travelcanary_ops.source_sync_runs
                    WHERE status = 'accepted' AND source IN ({placeholders})
                )
                WHERE row_number = 1
                """,
                selected,
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        reason = str(exc)
        return {source: _previous_context_unavailable(reason) for source in selected}

    for source, fetched_rows, finished_at in rows:
        default_context[str(source)] = {
            "previous_accepted_rows": int(fetched_rows),
            "relative_ratio": None,
            "previous_accepted_finished_at": _format_duckdb_value(finished_at),
            "previous_context_status": "available",
            "previous_context_reason": None,
        }
    return default_context


def _run_with_timing(
    source: str,
    contract: SourceContract,
    source_url: str,
    previous_context: dict[str, object],
    audit_fn: Callable[[], dict[str, object]],
) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    try:
        result = audit_fn()
    except Exception as exc:
        result = _error_result(source, contract, exc)
    finished_at = datetime.now(timezone.utc)
    result["source"] = source
    result["role"] = contract.role
    result["started_at"] = started_at.isoformat()
    result["finished_at"] = finished_at.isoformat()
    result["duration_seconds"] = round((finished_at - started_at).total_seconds(), 3)
    result["source_url"] = source_url
    previous_rows = previous_context.get("previous_accepted_rows")
    if previous_rows is not None:
        result["previous_accepted_rows"] = previous_rows
        result["relative_ratio"] = _relative_ratio(
            int(result.get("rows", 0)), int(previous_rows)
        )
    result["previous_accepted_finished_at"] = previous_context.get(
        "previous_accepted_finished_at"
    )
    if previous_context:
        result["previous_context_status"] = previous_context.get(
            "previous_context_status"
        )
        if previous_context.get("previous_context_reason"):
            result["previous_context_reason"] = previous_context[
                "previous_context_reason"
            ]
    return result


def _with_floor_proposal(
    result: dict[str, object], *, propose_floors: bool
) -> dict[str, object]:
    if not propose_floors:
        return result
    result["proposed_minimum_rows"] = (
        math.floor(int(result["rows"]) * 0.8)
        if result["status"] == "accepted"
        else None
    )
    return result


def audit_sources(
    target_date: date,
    *,
    selected_sources: Iterable[str] | None = None,
    propose_floors: bool = False,
    warehouse: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> list[dict[str, object]]:
    contracts = load_source_contracts()
    selected_list = select_sources(selected_sources, contracts)
    selected = set(selected_list)
    previous_contexts = _load_previous_context(warehouse, selected_list)
    advisory_fetchers = {source: fetcher for source, fetcher, _ in ADVISORY_SOURCES}
    results: list[dict[str, object]] = []
    for source in (source for source in AUDIT_SOURCE_ORDER if source in selected):
        previous_context = previous_contexts.get(source, {})
        previous_rows = previous_context.get("previous_accepted_rows")
        previous_rows = int(previous_rows) if previous_rows is not None else None
        if source == SOURCE_GDELT:
            detail = f" for {target_date.isoformat()}"
            audit_fn = partial(_audit_gdelt, target_date, previous_rows=previous_rows)
        else:
            detail = ""
            fetcher = advisory_fetchers[source]
            audit_fn = partial(
                _audit_advisory_fetcher,
                source,
                fetcher,
                previous_rows=previous_rows,
            )
        if progress:
            progress(f"auditing {source}{detail}")
        result = _run_with_timing(
            source,
            contracts[source],
            _source_url(source, target_date),
            previous_context,
            audit_fn,
        )
        results.append(_with_floor_proposal(result, propose_floors=propose_floors))
        if progress:
            progress(f"{source}: {result['status']}")
    return results


def has_blocking_required_failure(results: list[dict[str, object]]) -> bool:
    return any(
        row["role"] == "required" and row["status"] != "accepted" for row in results
    )


__all__ = [
    "ADVISORY_SOURCES",
    "AUDIT_SOURCE_ORDER",
    "audit_sources",
    "has_blocking_required_failure",
    "select_sources",
]
