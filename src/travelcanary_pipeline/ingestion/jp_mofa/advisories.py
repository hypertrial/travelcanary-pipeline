"""Japan MOFA overseas safety open data (XML)."""

from __future__ import annotations

import csv
import logging
from importlib import resources
from typing import Callable
from xml.etree.ElementTree import ParseError

import defusedxml.ElementTree as DefusedET
from defusedxml.common import DefusedXmlException

from travelcanary_pipeline.ingestion.common.advisory import (
    AdvisoryRow,
    BatchDiagnostics,
    row_from_parts,
    utc_now_iso,
)
from travelcanary_pipeline.ingestion.common.dlt_runner import sync_advisory_rows
from travelcanary_pipeline.naming import SOURCE_JP_MOFA
from travelcanary_pipeline.resources.http import get_text

JP_COUNTRY_XML_BASE = "https://www.ezairyu.mofa.go.jp/opendata/country"
LOGGER = logging.getLogger(__name__)


def _is_xml_payload(text: str) -> bool:
    stripped = text.lstrip("\ufeff").lstrip()
    return stripped.startswith("<?xml") or stripped.startswith("<opendata")


def _load_country_codes() -> list[dict[str, str]]:
    with (
        resources.files("travelcanary_pipeline.ingestion.jp_mofa")
        .joinpath("country_codes.csv")
        .open(encoding="utf-8") as handle
    ):
        return list(csv.DictReader(handle))


def _risk_level_count(root: DefusedET.Element, tag: str) -> int:
    text = root.findtext(tag)
    if not text:
        return 0
    try:
        return int(text.strip())
    except ValueError:
        return 0


def derive_jp_native_level(root: DefusedET.Element) -> str | None:
    """Return highest MOFA risk level with a non-zero regional count, or None."""
    for level in ("4", "3", "2", "1"):
        if _risk_level_count(root, f"riskLevel{level}") > 0:
            return level
    return None


def _jp_summary_text(root: DefusedET.Element) -> str | None:
    title = (root.findtext("riskTitle") or "").strip()
    lead = (root.findtext("riskLead") or "").strip()
    if title or lead:
        return "\n".join(part for part in (title, lead) if part)[:4000] or None
    widearea = root.find("wideareaSpot")
    if widearea is not None:
        spot_title = (widearea.findtext("title") or "").strip()
        spot_lead = (widearea.findtext("lead") or "").strip()
        if spot_title or spot_lead:
            return "\n".join(part for part in (spot_title, spot_lead) if part)[:4000]
    return None


def parse_jp_country_xml(
    country_code: str,
    xml_text: str,
    *,
    iso3: str | None = None,
    country_name: str | None = None,
    ingested_at: str | None = None,
) -> AdvisoryRow:
    ingested = ingested_at or utc_now_iso()
    root = DefusedET.fromstring(xml_text)
    native_level = derive_jp_native_level(root)
    return row_from_parts(
        source=SOURCE_JP_MOFA,
        destination_native_id=country_code,
        destination_iso3=iso3,
        destination_name=country_name,
        native_level=native_level,
        native_level_label=native_level,
        summary_text=_jp_summary_text(root),
        source_url=f"{JP_COUNTRY_XML_BASE}/{country_code}L.xml",
        published_at=root.attrib.get("lastModified"),
        ingested_at=ingested,
    )


def fetch_jp_advisories(
    *,
    country_codes: list[dict[str, str]] | None = None,
    fetch_text: Callable[[str], str] = get_text,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    ingested = utc_now_iso()
    rows: list[AdvisoryRow] = []
    diagnostics = diagnostics or BatchDiagnostics()
    for entry in country_codes or _load_country_codes():
        if not isinstance(entry, dict) or not str(entry.get("mofa_code") or "").strip():
            diagnostics.skip("invalid country-code entry")
            continue
        code = entry["mofa_code"]
        url = f"{JP_COUNTRY_XML_BASE}/{code}L.xml"
        xml_text = fetch_text(url)
        if not _is_xml_payload(xml_text):
            diagnostics.skip("non-XML advisory")
            LOGGER.warning("Skipping non-XML MOFA response for code %s", code)
            continue
        try:
            row = parse_jp_country_xml(
                code,
                xml_text,
                iso3=entry.get("iso3") or None,
                country_name=entry.get("name") or None,
                ingested_at=ingested,
            )
        except (ParseError, DefusedXmlException):
            diagnostics.skip("invalid XML advisory")
            continue
        diagnostics.observe()
        rows.append(row)
    return rows


def sync_jp_mofa_advisories(
    *,
    country_codes: list[dict[str, str]] | None = None,
    fetch_text: Callable[[str], str] = get_text,
    enforce_contract: bool = True,
) -> dict[str, object]:
    diagnostics = BatchDiagnostics()
    rows = fetch_jp_advisories(
        country_codes=country_codes,
        fetch_text=fetch_text,
        diagnostics=diagnostics,
    )
    return sync_advisory_rows(
        source=SOURCE_JP_MOFA,
        rows=rows,
        diagnostics=diagnostics,
        enforce_contract=enforce_contract,
    )


__all__ = [
    "JP_COUNTRY_XML_BASE",
    "derive_jp_native_level",
    "fetch_jp_advisories",
    "parse_jp_country_xml",
    "sync_jp_mofa_advisories",
]
