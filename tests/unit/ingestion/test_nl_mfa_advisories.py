from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.ingestion.nl_mfa.advisories import (
    fetch_nl_advisories,
    parse_nl_index_xml,
)


def test_parse_nl_index_xml_extracts_colour():
    xml = """<?xml version="1.0"?>
    <documents>
      <document>
        <id>THA</id>
        <title>Thailand</title>
        <canonical>https://example.com</canonical>
        <introduction><![CDATA[Het advies is oranje voor Thailand.]]></introduction>
      </document>
    </documents>"""
    rows = parse_nl_index_xml(xml, ingested_at="2026-07-01T00:00:00+00:00")
    assert rows[0]["native_level"] == "oranje"
    assert rows[0]["destination_iso3"] == "THA"
    assert rows[0]["destination_iso2"] is None


def test_fetch_nl_advisories_paginates_by_offset():
    def fetch_text(url: str) -> str:
        if "offset=25" in url:
            return """<documents>
              <document><id>FRA</id><introduction>groen</introduction></document>
            </documents>"""
        return "".join(
            ["<documents>"]
            + [
                f"<document><id>T{i:02d}</id><introduction>geel</introduction></document>"
                for i in range(25)
            ]
            + ["</documents>"]
        )

    rows = fetch_nl_advisories(fetch_text=fetch_text)
    assert len(rows) == 26
    assert rows[-1]["destination_native_id"] == "FRA"


def test_parse_nl_index_xml_missing_colour_and_duplicate_page_stop():
    xml = """<documents>
      <document>
        <id>xy</id>
        <title>Unknown</title>
        <introduction>No colour here</introduction>
      </document>
    </documents>"""
    rows = parse_nl_index_xml(xml, ingested_at="2026-07-01T00:00:00+00:00")
    assert rows[0]["native_level"] is None
    assert rows[0]["destination_iso3"] is None

    full_page = "".join(
        ["<documents>"]
        + [
            f"<document><id>T{i:02d}</id><introduction>geel</introduction></document>"
            for i in range(25)
        ]
        + ["</documents>"]
    )

    seen_urls: list[str] = []

    def fetch_text(url: str) -> str:
        seen_urls.append(url)
        return full_page

    assert len(fetch_nl_advisories(fetch_text=fetch_text)) == 25
    assert len(seen_urls) == 2


def test_nl_parser_reports_documents_without_identity():
    diagnostics = BatchDiagnostics()
    assert (
        parse_nl_index_xml(
            "<documents><document /></documents>", diagnostics=diagnostics
        )
        == []
    )
    assert diagnostics.skip_reasons == {"advisory missing identity": 1}


def test_nl_fetch_reports_invalid_xml_page():
    diagnostics = BatchDiagnostics()
    assert (
        fetch_nl_advisories(
            fetch_text=lambda _url: "<documents>", diagnostics=diagnostics
        )
        == []
    )
    assert diagnostics.skip_reasons == {"invalid XML page": 1}
