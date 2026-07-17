"""UK FCDO travel advice via GOV.UK Content API."""

from __future__ import annotations

import csv
import logging
from importlib import resources
from typing import Any, Callable

import requests

from travelcanary_pipeline.ingestion.common.advisory import (
    AdvisoryRow,
    BatchDiagnostics,
    row_from_parts,
    utc_now_iso,
)
from travelcanary_pipeline.ingestion.common.dlt_runner import sync_advisory_rows
from travelcanary_pipeline.ingestion.uk_fcdo.iso_resolver import resolve_iso
from travelcanary_pipeline.naming import SOURCE_UK_FCDO
from travelcanary_pipeline.resources.http import get_json

UK_CONTENT_API_BASE = "https://www.gov.uk/api/content/foreign-travel-advice"
LOGGER = logging.getLogger(__name__)


def _load_slug_seed() -> list[dict[str, str]]:
    with (
        resources.files("travelcanary_pipeline.ingestion.uk_fcdo")
        .joinpath("country_slugs.csv")
        .open(encoding="utf-8") as handle
    ):
        return list(csv.DictReader(handle))


def discover_uk_country_slugs(
    *,
    fetch_json: Callable[[str], object] = get_json,
    diagnostics: BatchDiagnostics | None = None,
) -> list[dict[str, str]]:
    """Return GOV.UK travel-advice slugs from the index API."""
    diagnostics = diagnostics or BatchDiagnostics()
    payload = fetch_json(UK_CONTENT_API_BASE)
    if not isinstance(payload, dict):
        diagnostics.skip("invalid index page")
        return []
    slugs: list[dict[str, str]] = []
    links = payload.get("links")
    if links is None:
        links = {}
    if not isinstance(links, dict):
        diagnostics.skip("invalid index page")
        return slugs
    children = links.get("children")
    if children is None:
        children = []
    if not isinstance(children, list):
        diagnostics.skip("invalid index child collection")
        return slugs
    for child in children:
        if not isinstance(child, dict):
            diagnostics.skip("non-object index child")
            continue
        details = child.get("details")
        if details is None:
            details = {}
        if not isinstance(details, dict):
            diagnostics.skip("invalid index child")
            continue
        country = details.get("country")
        if country is None:
            country = {}
        if not isinstance(country, dict):
            diagnostics.skip("invalid index child")
            continue
        slug = str(country.get("slug") or "").strip()
        if not slug:
            api_path = str(child.get("api_path") or "")
            if api_path.startswith("/api/content/foreign-travel-advice/"):
                slug = api_path.rsplit("/", 1)[-1]
        if slug:
            name = str(country.get("name") or "").strip()
            slugs.append({"slug": slug, "iso2": "", "iso3": "", "name": name})
        else:
            diagnostics.skip("index child missing slug")
    return slugs


def _merge_seed_metadata(slugs: list[dict[str, str]]) -> list[dict[str, str]]:
    seed_by_slug = {entry["slug"]: entry for entry in _load_slug_seed()}
    merged: list[dict[str, str]] = []
    for entry in slugs:
        slug = entry["slug"]
        seed = seed_by_slug.get(slug, {})
        merged.append(
            {
                "slug": slug,
                "iso2": entry.get("iso2") or seed.get("iso2") or "",
                "iso3": entry.get("iso3") or seed.get("iso3") or "",
                "name": entry.get("name") or seed.get("name") or "",
            }
        )
    return merged


def _resolve_missing_iso_codes(
    slugs: list[dict[str, str]],
) -> list[dict[str, str]]:
    resolved: list[dict[str, str]] = []
    for entry in slugs:
        iso2 = entry.get("iso2") or ""
        iso3 = entry.get("iso3") or ""
        if not iso2 or not iso3:
            resolved_iso2, resolved_iso3 = resolve_iso(
                entry["slug"],
                entry.get("name") or "",
            )
            iso2 = iso2 or resolved_iso2 or ""
            iso3 = iso3 or resolved_iso3 or ""
        resolved.append(
            {
                "slug": entry["slug"],
                "iso2": iso2,
                "iso3": iso3,
                "name": entry.get("name") or "",
            }
        )
    return resolved


def parse_uk_travel_advice(
    slug: str,
    payload: dict[str, Any],
    *,
    iso2: str | None = None,
    iso3: str | None = None,
    ingested_at: str | None = None,
) -> AdvisoryRow:
    ingested = ingested_at or utc_now_iso()
    details = payload.get("details") or {}
    alert_status = details.get("alert_status") or []
    native_level = str(alert_status[0]) if alert_status else None
    country = details.get("country") or {}
    return row_from_parts(
        source=SOURCE_UK_FCDO,
        destination_native_id=slug,
        destination_iso2=iso2,
        destination_iso3=iso3,
        destination_name=str(country.get("name") or payload.get("title") or slug),
        native_level=native_level,
        native_level_label=native_level,
        summary_text=str(payload.get("description") or "")[:4000] or None,
        source_url=f"https://www.gov.uk/foreign-travel-advice/{slug}",
        published_at=str(
            payload.get("public_updated_at") or details.get("updated_at") or ""
        )
        or None,
        ingested_at=ingested,
    )


def fetch_uk_advisories(
    *,
    slugs: list[dict[str, str]] | None = None,
    fetch_json: Callable[[str], object] = get_json,
    discover_all: bool = False,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    ingested = utc_now_iso()
    rows: list[AdvisoryRow] = []
    diagnostics = diagnostics or BatchDiagnostics()
    slug_entries = slugs
    if slug_entries is None:
        if discover_all:
            slug_entries = _resolve_missing_iso_codes(
                _merge_seed_metadata(
                    discover_uk_country_slugs(
                        fetch_json=fetch_json, diagnostics=diagnostics
                    )
                )
            )
        else:
            slug_entries = _load_slug_seed()
    for entry in slug_entries:
        if not isinstance(entry, dict) or not str(entry.get("slug") or "").strip():
            diagnostics.skip("invalid advisory index entry")
            continue
        slug = entry["slug"]
        url = f"{UK_CONTENT_API_BASE}/{slug}"
        try:
            payload = fetch_json(url)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                diagnostics.skip("withdrawn advisory (HTTP 404)", blocking=False)
                LOGGER.warning("Skipping withdrawn UK travel advice slug: %s", slug)
                continue
            raise
        if not isinstance(payload, dict):
            diagnostics.skip("non-object advisory")
            continue
        details = payload.get("details")
        if details is not None and not isinstance(details, dict):
            diagnostics.skip("invalid advisory details")
            continue
        diagnostics.observe()
        rows.append(
            parse_uk_travel_advice(
                slug,
                payload,
                iso2=entry.get("iso2") or None,
                iso3=entry.get("iso3") or None,
                ingested_at=ingested,
            )
        )
    return rows


def sync_uk_fcdo_advisories(
    *,
    slugs: list[dict[str, str]] | None = None,
    fetch_json: Callable[[str], object] = get_json,
    discover_all: bool = False,
    enforce_contract: bool = True,
) -> dict[str, object]:
    diagnostics = BatchDiagnostics()
    rows = fetch_uk_advisories(
        slugs=slugs,
        fetch_json=fetch_json,
        discover_all=discover_all,
        diagnostics=diagnostics,
    )
    return sync_advisory_rows(
        source=SOURCE_UK_FCDO,
        rows=rows,
        diagnostics=diagnostics,
        enforce_contract=enforce_contract,
    )


__all__ = [
    "UK_CONTENT_API_BASE",
    "discover_uk_country_slugs",
    "fetch_uk_advisories",
    "parse_uk_travel_advice",
    "sync_uk_fcdo_advisories",
]
