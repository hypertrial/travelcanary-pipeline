"""Offline compatibility checks for sanitized upstream payload shapes."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import pytest

from travelcanary_pipeline.ingestion.ca_gac.advisories import (
    CA_INDEX_URL,
    fetch_ca_advisories,
)
from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.ingestion.gdelt import events as gdelt_events
from travelcanary_pipeline.ingestion.jp_mofa.advisories import (
    JP_COUNTRY_XML_BASE,
    fetch_jp_advisories,
)
from travelcanary_pipeline.ingestion.nl_mfa.advisories import (
    NL_TRAVELADVICE_INDEX_URL,
    fetch_nl_advisories,
)
from travelcanary_pipeline.ingestion.uk_fcdo.advisories import (
    UK_CONTENT_API_BASE,
    fetch_uk_advisories,
)
from travelcanary_pipeline.ingestion.us_state.advisories import (
    US_TRAVEL_ADVISORIES_RSS_URL,
    US_TRAVEL_ADVISORIES_URL,
    fetch_us_advisories,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
CONTRACTS = FIXTURES / "contracts"
pytestmark = pytest.mark.contract


def _json(name: str):
    return json.loads((CONTRACTS / name).read_text(encoding="utf-8"))


def _clean(diagnostics: BatchDiagnostics, *, discovered: int = 1) -> None:
    assert diagnostics.summary() == {
        "discovered_rows": discovered,
        "skipped_rows": 0,
        "blocking_skipped_rows": 0,
        "skip_reasons": {},
    }


def _json_fetcher(expected_url: str, fixture: str):
    def fetch_json(url: str):
        assert url == expected_url
        return _json(fixture)

    return fetch_json


def _text_fetcher(expected_url: str, fixture: str):
    text = (CONTRACTS / fixture).read_text(encoding="utf-8")

    def fetch_text(url: str):
        assert url == expected_url
        return text

    return fetch_text


def test_canada_json_advisory_payload_contract():
    diagnostics = BatchDiagnostics()
    rows = fetch_ca_advisories(
        fetch_json=_json_fetcher(CA_INDEX_URL, "ca_gac.json"),
        diagnostics=diagnostics,
    )

    assert len(rows) == 1
    row = rows[0]
    assert (
        row["advisory_id"],
        row["destination_iso2"],
        row["destination_iso3"],
        row["native_level"],
        row["published_at"],
    ) == ("ca_gac:TH", "TH", None, "2", "2026-06-10T14:30:00Z")
    _clean(diagnostics)


def test_us_state_empty_json_falls_back_to_rss_payload_contract():
    diagnostics = BatchDiagnostics()

    def fetch_empty_json(url: str):
        assert url == US_TRAVEL_ADVISORIES_URL
        return []

    rows = fetch_us_advisories(
        fetch_json=fetch_empty_json,
        fetch_text=_text_fetcher(US_TRAVEL_ADVISORIES_RSS_URL, "us_state.xml"),
        diagnostics=diagnostics,
    )

    assert len(rows) == 1
    row = rows[0]
    assert (
        row["advisory_id"],
        row["destination_iso2"],
        row["destination_iso3"],
        row["native_level"],
        row["published_at"],
    ) == ("us_state:TH", "TH", "THA", "2", "Tue, 09 Jun 2026")
    _clean(diagnostics)


def test_netherlands_xml_payload_contract():
    diagnostics = BatchDiagnostics()
    rows = fetch_nl_advisories(
        fetch_text=_text_fetcher(f"{NL_TRAVELADVICE_INDEX_URL}?offset=0", "nl_mfa.xml"),
        diagnostics=diagnostics,
    )

    assert len(rows) == 1
    assert rows[0]["advisory_id"] == "nl_mfa:THA"
    assert rows[0]["destination_iso3"] == "THA"
    assert rows[0]["native_level"] == "geel"
    assert rows[0]["published_at"] is None
    _clean(diagnostics)


def test_japan_xml_payload_contract():
    diagnostics = BatchDiagnostics()
    rows = fetch_jp_advisories(
        country_codes=[{"mofa_code": "0066", "iso3": "THA", "name": "Thailand"}],
        fetch_text=_text_fetcher(f"{JP_COUNTRY_XML_BASE}/0066L.xml", "jp_mofa.xml"),
        diagnostics=diagnostics,
    )

    assert len(rows) == 1
    assert rows[0]["advisory_id"] == "jp_mofa:0066"
    assert rows[0]["destination_iso3"] == "THA"
    assert rows[0]["native_level"] == "2"
    assert rows[0]["published_at"] == "2026/06/12 09:00:00"
    _clean(diagnostics)


def test_uk_discovery_and_detail_payload_contract():
    diagnostics = BatchDiagnostics()

    def fetch_json(url: str):
        if url == UK_CONTENT_API_BASE:
            return _json("uk_fcdo_index.json")
        if url == f"{UK_CONTENT_API_BASE}/thailand":
            return _json("uk_fcdo_thailand.json")
        raise AssertionError(f"unexpected URL: {url}")

    rows = fetch_uk_advisories(
        fetch_json=fetch_json, discover_all=True, diagnostics=diagnostics
    )

    assert len(rows) == 1
    assert rows[0]["advisory_id"] == "uk_fcdo:thailand"
    assert rows[0]["destination_iso2"] == "TH"
    assert rows[0]["destination_iso3"] == "THA"
    assert rows[0]["native_level"] == "avoid_all_travel_to_parts"
    assert rows[0]["published_at"] == "2026-06-13T08:45:00Z"
    _clean(diagnostics)


def test_gdelt_zip_payload_contract(monkeypatch):
    csv_fixture = FIXTURES / "gdelt" / "20260701.export.CSV"
    zipped = io.BytesIO()
    with zipfile.ZipFile(zipped, "w") as archive:
        archive.writestr(csv_fixture.name, csv_fixture.read_bytes())
    zip_bytes = zipped.getvalue()
    expected_url = gdelt_events.GDELT_DAILY_EXPORT_URL.format(yyyymmdd="20260701")
    assert expected_url.startswith(
        "https://storage.googleapis.com/data.gdeltproject.org/"
    )

    def fetch_file(url: str, destination):
        assert url == expected_url
        destination.write(zip_bytes)

    monkeypatch.setattr(gdelt_events, "prune_gdelt_events", lambda _conn: 0)
    conn = duckdb.connect()
    try:
        result = gdelt_events.sync_gdelt_daily_events(
            for_date=date(2026, 7, 1),
            fetch_file=fetch_file,
            conn=conn,
            enforce_contract=False,
        )
        row = conn.execute(
            """
            select global_event_id, event_date, action_geo_country_code,
                   event_root_code, date_added
            from gdelt_raw.events
            """
        ).fetchone()
    finally:
        conn.close()

    assert row == (
        "900000001",
        "20260701",
        "TH",
        "19",
        "20260701120000",
    )
    assert result["url"] == expected_url
    assert result["rows"] == 1
    assert result["pruned_rows"] == 0
    assert result["discovered_rows"] == 1
    assert result["skipped_rows"] == 0
    assert result["blocking_skipped_rows"] == 0
    assert result["skip_reasons"] == {}
