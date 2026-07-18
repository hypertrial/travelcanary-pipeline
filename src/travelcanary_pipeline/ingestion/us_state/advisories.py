"""US State Department travel advisories."""

from __future__ import annotations

import csv
import html
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Callable
from urllib.parse import urlparse
from xml.etree import ElementTree

import pycountry

from travelcanary_pipeline.config.settings import BASE_DIR
from travelcanary_pipeline.ingestion.common.advisory import (
    AdvisoryRow,
    BatchDiagnostics,
    parse_us_level,
    row_from_parts,
    utc_now_iso,
)
from travelcanary_pipeline.ingestion.common.dlt_runner import sync_advisory_rows
from travelcanary_pipeline.ingestion.uk_fcdo.iso_resolver import resolve_iso
from travelcanary_pipeline.naming import SOURCE_US_STATE
from travelcanary_pipeline.resources.http import get_json, get_text

US_TRAVEL_ADVISORIES_URL = "https://cadataapi.state.gov/api/TravelAdvisories"
US_TRAVEL_ADVISORIES_RSS_URL = "https://travel.state.gov/_res/rss/TAsTWs.xml"
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_US_FIPS_OVERRIDES = {
    "A1": "BES",
    "A2": "GUF",
    "LO": "SVK",
    "NN": "SXM",
    "UC": "CUW",
}
_US_TITLE_OVERRIDES = {
    "Burma": "MMR",
    "Côte d'Ivoire (Ivory-Coast)": "CIV",
    "Democratic Republic of the Congo": "COD",
    "Hong Kong": "HKG",
    "Macau": "MAC",
    "The Bahamas": "BHS",
    "The Gambia": "GMB",
    "The Kyrgyz Republic": "KGZ",
    "Vatican City (Holy See)": "VAT",
}
_US_SLUG_OVERRIDES = {
    "bonaire": "BES",
    "cote-d-ivoire": "CIV",
    "curacao": "CUW",
    "macau": "MAC",
    "saba": "BES",
    "sint-maarten": "SXM",
    "sint-eustatius": "BES",
}


def _fips_to_iso3() -> dict[str, str]:
    path = BASE_DIR / "dbt" / "seeds" / "fips10_4_to_iso3.csv"
    with path.open(encoding="utf-8") as handle:
        return {row["fips10_4"]: row["iso3"] for row in csv.DictReader(handle)}


def _first_country_identity(categories: list[object]) -> tuple[str | None, str | None]:
    fips = _fips_to_iso3()
    for category in categories:
        candidate = str(category or "").upper()
        if candidate in _US_FIPS_OVERRIDES:
            country = pycountry.countries.get(alpha_3=_US_FIPS_OVERRIDES[candidate])
            return (country.alpha_2 if country else None), _US_FIPS_OVERRIDES[candidate]
        if candidate in fips:
            country = pycountry.countries.get(alpha_3=fips[candidate])
            return (country.alpha_2 if country else None), fips[candidate]
        if len(candidate) == 2 and pycountry.countries.get(alpha_2=candidate):
            country = pycountry.countries.get(alpha_2=candidate)
            return candidate, country.alpha_3 if country else None
    return None, None


def _title_country_identity(
    title: str, source_url: str
) -> tuple[str | None, str | None]:
    country_name = title.split(" - ")[0].strip()
    slug = urlparse(source_url).path.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"-travel-advisory\d*$", "", slug.removesuffix(".html"))
    if slug.startswith("destination."):
        slug = slug.removeprefix("destination.")
    override = _US_TITLE_OVERRIDES.get(country_name) or _US_SLUG_OVERRIDES.get(slug)
    if override:
        country = pycountry.countries.get(alpha_3=override)
        return (country.alpha_2 if country else None), override
    iso2, iso3 = resolve_iso(slug, country_name)
    if iso3:
        return iso2, iso3
    return resolve_iso(slug, slug.replace("-", " "))


def _resolve_country_identity(
    title: str, source_url: str, categories: list[object]
) -> tuple[str | None, str | None]:
    """Prefer title/slug identity when it conflicts with a FIPS country tag.

    The empty JSON catalog falls back to RSS, where Country-Tag values are FIPS
    codes that can disagree with the destination named in the title.
    """
    fips_iso2, fips_iso3 = _first_country_identity(categories)
    title_iso2, title_iso3 = _title_country_identity(title, source_url)
    if title_iso3 and (not fips_iso3 or fips_iso3 != title_iso3):
        return title_iso2, title_iso3
    if fips_iso3:
        return fips_iso2, fips_iso3
    return title_iso2, title_iso3


def _published_sort_key(value: str | None) -> float:
    if not value:
        return float("-inf")
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError, IndexError):
        pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return float("-inf")


def _us_row_rank(row: AdvisoryRow) -> tuple[float, int, str]:
    url = str(row.get("source_url") or "")
    prefers_classic = 0 if "tsg_aem" in url else 1
    return (_published_sort_key(row.get("published_at")), prefers_classic, url)


def dedupe_us_advisory_rows(rows: list[AdvisoryRow]) -> list[AdvisoryRow]:
    """Keep one row per ``advisory_id``, preferring newer classic catalog URLs."""
    chosen: dict[str, AdvisoryRow] = {}
    order: list[str] = []
    for row in rows:
        key = row["advisory_id"]
        if key not in chosen:
            chosen[key] = row
            order.append(key)
            continue
        if _us_row_rank(row) > _us_row_rank(chosen[key]):
            chosen[key] = row
    return [chosen[key] for key in order]


def parse_us_advisories(
    payload: list[object],
    *,
    ingested_at: str | None = None,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    ingested = ingested_at or utc_now_iso()
    diagnostics = diagnostics or BatchDiagnostics()
    rows: list[AdvisoryRow] = []
    for item in payload:
        if not isinstance(item, dict):
            diagnostics.skip("non-object advisory")
            continue
        diagnostics.observe()
        title = str(item.get("Title") or "")
        native_level, native_label = parse_us_level(title)
        categories = item.get("Category") or []
        source_url = str(item.get("Link") or "")
        iso2, iso3 = _resolve_country_identity(title, source_url, categories)
        destination_id = iso2 or title
        rows.append(
            row_from_parts(
                source=SOURCE_US_STATE,
                destination_native_id=destination_id,
                destination_iso2=iso2,
                destination_iso3=iso3,
                destination_name=title.split(" - ")[0].strip()
                if " - " in title
                else None,
                native_level=native_level,
                native_level_label=native_label,
                summary_text=str(item.get("Summary") or "")[:4000] or None,
                source_url=source_url or None,
                published_at=str(item.get("Updated") or item.get("Published") or "")
                or None,
                ingested_at=ingested,
            )
        )
    return dedupe_us_advisory_rows(rows)


def parse_us_rss(
    xml_text: str,
    *,
    ingested_at: str | None = None,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        raise ValueError("US advisories RSS payload must be valid XML") from exc

    payload: list[object] = []
    for item in root.findall("./channel/item"):
        categories = [
            category.text
            for category in item.findall("category")
            if category.get("domain") == "Country-Tag" and category.text
        ]
        description = item.findtext("description") or ""
        payload.append(
            {
                "Title": item.findtext("title") or "",
                "Category": categories,
                "Summary": html.unescape(_HTML_TAG_RE.sub(" ", description)).strip(),
                "Link": item.findtext("link") or "",
                "Published": item.findtext("pubDate") or "",
            }
        )
    return parse_us_advisories(
        payload,
        ingested_at=ingested_at,
        diagnostics=diagnostics,
    )


def fetch_us_advisories(
    url: str = US_TRAVEL_ADVISORIES_URL,
    *,
    fetch_json: Callable[[str], object] = get_json,
    fetch_text: Callable[[str], str] = get_text,
    diagnostics: BatchDiagnostics | None = None,
) -> list[AdvisoryRow]:
    payload = fetch_json(url)
    if not isinstance(payload, list):
        raise ValueError("US advisories payload must be a JSON list")
    if not payload:
        return parse_us_rss(
            fetch_text(US_TRAVEL_ADVISORIES_RSS_URL),
            diagnostics=diagnostics,
        )
    return parse_us_advisories(payload, diagnostics=diagnostics)


def sync_us_state_advisories(
    *,
    url: str = US_TRAVEL_ADVISORIES_URL,
    fetch_json: Callable[[str], object] = get_json,
    fetch_text: Callable[[str], str] = get_text,
    enforce_contract: bool = True,
) -> dict[str, object]:
    diagnostics = BatchDiagnostics()
    rows = fetch_us_advisories(
        url,
        fetch_json=fetch_json,
        fetch_text=fetch_text,
        diagnostics=diagnostics,
    )
    return sync_advisory_rows(
        source=SOURCE_US_STATE,
        rows=rows,
        diagnostics=diagnostics,
        url=url,
        enforce_contract=enforce_contract,
    )


__all__ = [
    "US_TRAVEL_ADVISORIES_URL",
    "US_TRAVEL_ADVISORIES_RSS_URL",
    "dedupe_us_advisory_rows",
    "fetch_us_advisories",
    "parse_us_advisories",
    "parse_us_rss",
    "sync_us_state_advisories",
]
