"""Netherlands MFA travel advice via open data XML feed."""

from __future__ import annotations

import re
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
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
from travelcanary_pipeline.naming import SOURCE_NL_MFA
from travelcanary_pipeline.resources.http import get_text

NL_TRAVELADVICE_INDEX_URL = (
    "https://opendata.nederlandwereldwijd.nl/v2/sources/nederlandwereldwijd/"
    "infotypes/traveladvice"
)
NL_PAGE_SIZE = 25

_COLOUR_RE = re.compile(r"(groen|geel|oranje|rood)", re.IGNORECASE)


def _extract_colour(text: str) -> tuple[str | None, str | None]:
    match = _COLOUR_RE.search(text or "")
    if not match:
        return None, None
    colour = match.group(1).lower()
    return colour, colour


def parse_nl_index_xml(
    xml_text: str,
    *,
    ingested_at: str | None = None,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    ingested = ingested_at or utc_now_iso()
    diagnostics = diagnostics or BatchDiagnostics()
    root = DefusedET.fromstring(xml_text)
    rows: list[AdvisoryRow] = []
    for document in root.findall(".//document"):
        doc_id = (document.findtext("id") or "").strip()
        title = (document.findtext("title") or "").strip()
        if not doc_id and not title:
            diagnostics.skip("advisory missing identity")
            continue
        diagnostics.observe()
        canonical = (document.findtext("canonical") or "").strip() or None
        intro = (document.findtext("introduction") or "").strip()
        native_level, native_label = _extract_colour(intro)
        rows.append(
            row_from_parts(
                source=SOURCE_NL_MFA,
                destination_native_id=doc_id or title,
                destination_iso3=doc_id.upper() if len(doc_id) == 3 else None,
                destination_name=title or None,
                native_level=native_level,
                native_level_label=native_label,
                summary_text=intro[:4000] or None,
                source_url=canonical,
                published_at=None,
                ingested_at=ingested,
            )
        )
    return rows


def _url_with_offset(url: str, offset: int) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["offset"] = str(offset)
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
    )


def fetch_nl_advisories(
    url: str = NL_TRAVELADVICE_INDEX_URL,
    *,
    fetch_text: Callable[[str], str] = get_text,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    diagnostics = diagnostics or BatchDiagnostics()
    rows: list[AdvisoryRow] = []
    seen: set[str] = set()
    offset = 0
    while True:
        xml_text = fetch_text(_url_with_offset(url, offset))
        try:
            page_rows = parse_nl_index_xml(xml_text, diagnostics=diagnostics)
        except (ParseError, DefusedXmlException):
            diagnostics.skip("invalid XML page")
            return rows
        new_rows = [row for row in page_rows if row["advisory_id"] not in seen]
        rows.extend(new_rows)
        seen.update(row["advisory_id"] for row in new_rows)
        if len(page_rows) < NL_PAGE_SIZE:
            return rows
        if not new_rows:
            return rows
        offset += NL_PAGE_SIZE


def sync_nl_mfa_advisories(
    *,
    url: str = NL_TRAVELADVICE_INDEX_URL,
    fetch_text: Callable[[str], str] = get_text,
    enforce_contract: bool = True,
) -> dict[str, object]:
    diagnostics = BatchDiagnostics()
    rows = fetch_nl_advisories(url, fetch_text=fetch_text, diagnostics=diagnostics)
    return sync_advisory_rows(
        source=SOURCE_NL_MFA,
        rows=rows,
        diagnostics=diagnostics,
        url=url,
        enforce_contract=enforce_contract,
    )


__all__ = [
    "NL_TRAVELADVICE_INDEX_URL",
    "fetch_nl_advisories",
    "parse_nl_index_xml",
    "sync_nl_mfa_advisories",
]
