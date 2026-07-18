import defusedxml.ElementTree as DefusedET

from travelcanary_pipeline.ingestion.jp_mofa.advisories import (
    _load_country_codes,
    derive_jp_native_level,
    fetch_jp_advisories,
    parse_jp_country_xml,
)


def _fixture_xml(
    *,
    risk4: int = 0,
    risk3: int = 0,
    risk2: int = 0,
    risk1: int = 0,
    risk_title: str = "",
    risk_lead: str = "",
    widearea_title: str = "",
    widearea_lead: str = "",
) -> str:
    widearea = ""
    if widearea_title or widearea_lead:
        widearea = f"""
    <wideareaSpot>
      <title>{widearea_title}</title>
      <lead>{widearea_lead}</lead>
    </wideareaSpot>"""
    return f"""<?xml version="1.0"?>
<opendata lastModified="2026/07/10 03:56:47">
  <riskLevel4>{risk4}</riskLevel4>
  <riskLevel3>{risk3}</riskLevel3>
  <riskLevel2>{risk2}</riskLevel2>
  <riskLevel1>{risk1}</riskLevel1>
  <riskTitle>{risk_title}</riskTitle>
  <riskLead>{risk_lead}</riskLead>{widearea}
</opendata>"""


def test_derive_jp_native_level_no_active_advisory():
    root = DefusedET.fromstring(_fixture_xml())
    assert derive_jp_native_level(root) is None


def test_derive_jp_native_level_single_level():
    root = DefusedET.fromstring(_fixture_xml(risk2=2))
    assert derive_jp_native_level(root) == "2"


def test_derive_jp_native_level_mixed_levels_returns_highest():
    root = DefusedET.fromstring(_fixture_xml(risk3=1, risk4=1))
    assert derive_jp_native_level(root) == "4"


def test_parse_jp_country_xml_reads_level_and_headline():
    row = parse_jp_country_xml(
        "0380",
        _fixture_xml(
            risk4=1,
            risk_title="Ukraine advisory",
            risk_lead="Evacuation recommended.",
        ),
        iso3="UKR",
        country_name="Ukraine",
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    assert row["native_level"] == "4"
    assert row["destination_iso3"] == "UKR"
    assert "Ukraine advisory" in (row["summary_text"] or "")
    assert "Evacuation recommended." in (row["summary_text"] or "")


def test_parse_jp_country_xml_no_advisory_has_null_level():
    row = parse_jp_country_xml(
        "1000",
        _fixture_xml(
            widearea_title="Regional bulletin", widearea_lead="General notice"
        ),
        iso3="USA",
        country_name="United States",
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    assert row["native_level"] is None
    assert "Regional bulletin" in (row["summary_text"] or "")


def test_jp_parser_handles_invalid_counts_and_fetch_skips_non_xml(caplog):
    root = DefusedET.fromstring(
        _fixture_xml().replace("<riskLevel3>0", "<riskLevel3>x")
    )
    assert derive_jp_native_level(root) is None

    rows = fetch_jp_advisories(
        country_codes=[
            {"mofa_code": "bad", "iso3": "BAD", "name": "Bad"},
            {"mofa_code": "0380", "iso3": "UKR", "name": "Ukraine"},
        ],
        fetch_text=lambda url: (
            "not xml" if url.endswith("/badL.xml") else _fixture_xml(risk1=1)
        ),
    )

    assert len(rows) == 1
    assert rows[0]["destination_iso3"] == "UKR"
    assert "Skipping non-XML" in caplog.text

    no_summary = parse_jp_country_xml(
        "1000",
        _fixture_xml(widearea_title="", widearea_lead=""),
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    assert no_summary["summary_text"] is None

    empty_widearea = parse_jp_country_xml(
        "1000",
        """<opendata>
          <riskLevel4>0</riskLevel4>
          <riskLevel3>0</riskLevel3>
          <riskLevel2>0</riskLevel2>
          <riskLevel1>0</riskLevel1>
          <wideareaSpot><title></title><lead></lead></wideareaSpot>
        </opendata>""",
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    assert empty_widearea["summary_text"] is None


def test_fetch_jp_advisories_reports_when_all_payloads_are_malformed():
    from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics

    diagnostics = BatchDiagnostics()
    rows = fetch_jp_advisories(
        country_codes=[{"mofa_code": "bad", "iso3": "BAD", "name": "Bad"}],
        fetch_text=lambda _url: "not xml",
        diagnostics=diagnostics,
    )

    assert rows == []
    assert diagnostics.blocking_skipped_rows == 1
    assert diagnostics.skip_reasons == {"non-XML advisory": 1}


def test_fetch_jp_advisories_dedupes_duplicate_iso3_preferring_higher_risk():
    rows = fetch_jp_advisories(
        country_codes=[
            {"mofa_code": "1808", "iso3": "USA", "name": "United States"},
            {"mofa_code": "1000", "iso3": "USA", "name": "United States"},
            {"mofa_code": "1684", "iso3": "WSM", "name": "Samoa"},
            {"mofa_code": "0685", "iso3": "WSM", "name": "Samoa"},
            {"mofa_code": "9999", "iso3": "", "name": "Unresolved"},
        ],
        fetch_text=lambda url: _fixture_xml(
            risk2=1 if "1000" in url or "0685" in url else 0,
            risk_title="title",
        ),
    )

    assert [
        (row["destination_iso3"], row["destination_native_id"], row["native_level"])
        for row in rows
    ] == [
        ("USA", "1000", "2"),
        ("WSM", "0685", "2"),
        (None, "9999", None),
    ]

    from travelcanary_pipeline.ingestion.jp_mofa.advisories import (
        dedupe_jp_rows_by_iso3,
    )

    kept = dedupe_jp_rows_by_iso3(
        [
            rows[0],
            {
                **rows[0],
                "destination_native_id": "1808",
                "advisory_id": "jp_mofa:1808",
                "native_level": "1",
            },
        ]
    )
    assert kept[0]["destination_native_id"] == "1000"


def test_load_country_codes_reads_packaged_csv():
    codes = _load_country_codes()
    assert any(row["mofa_code"] == "0380" for row in codes)
    iso3_counts: dict[str, int] = {}
    for row in codes:
        iso3 = row["iso3"]
        if iso3:
            iso3_counts[iso3] = iso3_counts.get(iso3, 0) + 1
    assert all(count == 1 for count in iso3_counts.values())
