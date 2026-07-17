from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.ingestion.jp_mofa.advisories import fetch_jp_advisories


def test_fetch_jp_advisories_skips_non_xml_payloads():
    def fake_fetch(url: str):
        if url.endswith("1000L.xml"):
            return """<?xml version="1.0"?>
            <opendata lastModified="2026/07/10">
              <riskLevel4>0</riskLevel4>
              <riskLevel3>0</riskLevel3>
              <riskLevel2>1</riskLevel2>
              <riskLevel1>0</riskLevel1>
              <riskTitle>Sample advisory</riskTitle>
              <riskLead>Sample lead</riskLead>
            </opendata>"""
        return "<!doctype html><html></html>"

    rows = fetch_jp_advisories(
        country_codes=[
            {"mofa_code": "1000", "iso3": "USA", "name": "United States"},
            {"mofa_code": "9999", "iso3": "XXX", "name": "Bad"},
        ],
        fetch_text=fake_fetch,
    )
    assert len(rows) == 1
    assert rows[0]["destination_native_id"] == "1000"
    assert rows[0]["native_level"] == "2"


def test_fetch_jp_advisories_reports_an_all_malformed_batch():
    diagnostics = BatchDiagnostics()
    assert (
        fetch_jp_advisories(
            country_codes=[{"mofa_code": "9999", "iso3": "XXX", "name": "Bad"}],
            fetch_text=lambda _url: "<html></html>",
            diagnostics=diagnostics,
        )
        == []
    )
    assert diagnostics.summary() == {
        "discovered_rows": 1,
        "skipped_rows": 1,
        "blocking_skipped_rows": 1,
        "skip_reasons": {"non-XML advisory": 1},
    }


def test_fetch_jp_advisories_reports_invalid_entries_and_malformed_xml():
    diagnostics = BatchDiagnostics()
    rows = fetch_jp_advisories(
        country_codes=[{}, {"mofa_code": "1000", "iso3": "USA", "name": "USA"}],
        fetch_text=lambda _url: "<?xml version='1.0'?><opendata>",
        diagnostics=diagnostics,
    )

    assert rows == []
    assert diagnostics.skip_reasons == {
        "invalid XML advisory": 1,
        "invalid country-code entry": 1,
    }
