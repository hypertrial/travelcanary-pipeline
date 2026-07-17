"""Canada Global Affairs travel advisories."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from travelcanary_pipeline.ingestion.common.advisory import (
    AdvisoryRow,
    BatchDiagnostics,
    row_from_parts,
    utc_now_iso,
)
from travelcanary_pipeline.ingestion.common.dlt_runner import sync_advisory_rows
from travelcanary_pipeline.naming import SOURCE_CA_GAC
from travelcanary_pipeline.resources.http import get_json

CA_INDEX_URL = "https://data.international.gc.ca/travel-voyage/index-alpha-eng.json"

_CA_LEVELS = {
    1: ("1", "Exercise normal security precautions"),
    2: ("2", "Exercise a high degree of caution"),
    3: ("3", "Avoid non-essential travel"),
    4: ("4", "Avoid all travel"),
}


def _parse_ca_timestamp(value: object) -> str | None:
    if isinstance(value, dict):
        return str(value.get("date") or value.get("asp") or "") or None
    if value is None:
        return None
    return str(value)


def parse_ca_index(
    payload: dict[str, Any],
    *,
    ingested_at: str | None = None,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    ingested = ingested_at or utc_now_iso()
    diagnostics = diagnostics or BatchDiagnostics()
    data = payload.get("data")
    if data is None:
        data = {}
    rows: list[AdvisoryRow] = []
    if not isinstance(data, dict):
        diagnostics.skip("invalid advisory collection")
        return rows
    for iso2, item in data.items():
        if not isinstance(item, dict):
            diagnostics.skip("non-object advisory")
            continue
        diagnostics.observe()
        advisory_state = item.get("advisory-state")
        level_tuple = (
            _CA_LEVELS.get(int(advisory_state)) if advisory_state is not None else None
        )
        native_level, native_label = level_tuple if level_tuple else (None, None)
        rows.append(
            row_from_parts(
                source=SOURCE_CA_GAC,
                destination_native_id=str(iso2).upper(),
                destination_iso2=str(iso2).upper(),
                destination_name=str(item.get("country-eng") or "") or None,
                native_level=native_level,
                native_level_label=native_label,
                summary_text=None,
                source_url=CA_INDEX_URL,
                published_at=_parse_ca_timestamp(item.get("date-published")),
                ingested_at=ingested,
            )
        )
    return rows


def fetch_ca_advisories(
    url: str = CA_INDEX_URL,
    *,
    fetch_json: Callable[[str], object] = get_json,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    payload = fetch_json(url)
    if not isinstance(payload, dict):
        raise ValueError("Canada index payload must be a JSON object")
    return parse_ca_index(payload, diagnostics=diagnostics)


def sync_ca_gac_advisories(
    *,
    url: str = CA_INDEX_URL,
    fetch_json: Callable[[str], object] = get_json,
    enforce_contract: bool = True,
) -> dict[str, object]:
    diagnostics = BatchDiagnostics()
    rows = fetch_ca_advisories(url, fetch_json=fetch_json, diagnostics=diagnostics)
    return sync_advisory_rows(
        source=SOURCE_CA_GAC,
        rows=rows,
        diagnostics=diagnostics,
        url=url,
        generated_at=datetime.now(timezone.utc).isoformat(),
        enforce_contract=enforce_contract,
    )


__all__ = [
    "CA_INDEX_URL",
    "fetch_ca_advisories",
    "parse_ca_index",
    "sync_ca_gac_advisories",
]
