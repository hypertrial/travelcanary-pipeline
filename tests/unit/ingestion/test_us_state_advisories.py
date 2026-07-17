import pytest

from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.ingestion.us_state.advisories import (
    US_TRAVEL_ADVISORIES_RSS_URL,
    fetch_us_advisories,
    parse_us_advisories,
    parse_us_rss,
)


def test_parse_us_advisories_extracts_level_and_iso2():
    rows = parse_us_advisories(
        [
            {
                "Title": "Thailand - Level 3: Reconsider Travel",
                "Category": ["TH"],
                "Summary": "sample",
                "Link": "https://example.com",
                "Updated": "2026-07-01",
            },
            {
                "Title": "Aruba - Level 1: Exercise Normal Precautions",
                "Category": ["AW"],
            },
        ],
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    assert len(rows) == 2
    assert rows[0]["destination_iso2"] == "TH"
    assert rows[0]["native_level"] == "3"
    assert rows[1]["destination_iso3"] == "ABW"


def test_parse_us_advisories_fips_and_title_overrides():
    rows = parse_us_advisories(
        [
            {
                "Title": "Curacao - Level 2: Exercise Increased Caution",
                "Category": ["UC"],
            },
            {
                "Title": "Hong Kong - Level 2: Exercise Increased Caution",
                "Category": [],
            },
            {"Title": "Unknown", "Category": ["ZZ"]},
        ],
        ingested_at="2026-07-01T00:00:00+00:00",
    )

    assert rows[0]["destination_iso3"] == "CUW"
    assert rows[1]["destination_iso3"] == "HKG"
    assert rows[2]["destination_iso3"] is None
    assert rows[2]["destination_name"] is None


@pytest.mark.parametrize(
    ("title", "slug", "expected_iso3"),
    [
        ("CuraÃ§ao", "curacao", "CUW"),
        ("Saba", "saba", "BES"),
        ("Sint Eustatius", "sint-eustatius", "BES"),
    ],
)
def test_parse_us_advisories_resolves_current_catalog_slugs(title, slug, expected_iso3):
    rows = parse_us_advisories(
        [
            {
                "Title": f"{title} - Level 1: Exercise Normal Precautions",
                "Category": [],
                "Link": (
                    "https://travel.state.gov/content/travel/en/traveladvisories/"
                    f"traveladvisories/{slug}-travel-advisory.html"
                ),
            }
        ],
        ingested_at="2026-07-01T00:00:00+00:00",
    )

    assert rows[0]["destination_iso3"] == expected_iso3


def test_fetch_us_advisories_rejects_invalid_payload_shape():
    with pytest.raises(ValueError, match="JSON list"):
        fetch_us_advisories(fetch_json=lambda _url: {})


def test_fetch_us_advisories_falls_back_to_official_rss_for_empty_json():
    xml = """\
    <rss><channel><item>
      <title>Thailand - Level 2: Exercise Increased Caution</title>
      <link>https://travel.state.gov/thailand.html</link>
      <pubDate>Tue, 09 Jun 2026</pubDate>
      <description><![CDATA[<p>Exercise <b>increased caution</b>.</p>]]></description>
      <category domain="Threat-Level">Level 2</category>
      <category domain="Country-Tag">TH</category>
    </item></channel></rss>
    """

    def fetch_text(url: str) -> str:
        assert url == US_TRAVEL_ADVISORIES_RSS_URL
        return xml

    rows = fetch_us_advisories(
        fetch_json=lambda _url: [],
        fetch_text=fetch_text,
    )

    assert rows[0]["destination_iso3"] == "THA"
    assert rows[0]["native_level"] == "2"
    assert rows[0]["summary_text"] == "Exercise  increased caution ."


def test_parse_us_rss_resolves_country_name_without_country_tag():
    name_rows = parse_us_rss(
        """\
        <rss><channel><item>
          <title>Thailand - Level 2: Exercise Increased Caution</title>
          <link>https://travel.state.gov/thailand-travel-advisory.html</link>
          <category domain="Keyword">advisory</category>
        </item></channel></rss>
        """
    )

    assert name_rows[0]["destination_iso2"] == "TH"
    assert name_rows[0]["destination_iso3"] == "THA"

    slug_rows = parse_us_rss(
        """\
        <rss><channel>
          <item>
            <title>Mexico Travel Advisory - Level 2</title>
            <link>https://travel.state.gov/mexico-travel-advisory.html</link>
          </item>
          <item>
            <title>Mainland China, Hong Kong &amp; Macau - Level 2</title>
            <link>https://travel.state.gov/macau-travel-advisory1.html</link>
          </item>
          <item>
            <title>Unmapped region - Level 1</title>
            <link>https://travel.state.gov/unmapped-region.html</link>
          </item>
        </channel></rss>
        """
    )

    assert [row["destination_iso3"] for row in slug_rows] == ["MEX", "MAC", None]


def test_parse_us_rss_rejects_invalid_xml_and_allows_empty_channel():
    with pytest.raises(ValueError, match="valid XML"):
        parse_us_rss("<rss>")
    assert parse_us_rss("<rss><channel /></rss>") == []


def test_parse_us_advisories_reports_non_object_records():
    diagnostics = BatchDiagnostics()
    assert parse_us_advisories(["bad"], diagnostics=diagnostics) == []
    assert diagnostics.blocking_skipped_rows == 1
