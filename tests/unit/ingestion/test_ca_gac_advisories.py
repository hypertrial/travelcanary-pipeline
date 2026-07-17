import pytest

from travelcanary_pipeline.ingestion.ca_gac.advisories import (
    fetch_ca_advisories,
    parse_ca_index,
)
from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics


def test_parse_ca_index_maps_advisory_state():
    payload = {
        "data": {
            "TH": {
                "country-eng": "Thailand",
                "advisory-state": 3,
                "date-published": {"date": "2026-07-01"},
            }
        }
    }
    rows = parse_ca_index(payload, ingested_at="2026-07-01T00:00:00+00:00")
    assert rows[0]["native_level"] == "3"
    assert rows[0]["destination_iso2"] == "TH"


def test_parse_ca_index_skips_non_dict_and_timestamp_variants():
    diagnostics = BatchDiagnostics()
    rows = parse_ca_index(
        {
            "data": {
                "XX": "bad",
                "CH": {"country-eng": "Switzerland", "date-published": None},
                "DE": {"country-eng": "Germany", "date-published": "2026-07-02"},
                "FR": {
                    "country-eng": "France",
                    "advisory-state": 9,
                    "date-published": {"asp": "2026-07-03"},
                },
            }
        },
        ingested_at="2026-07-01T00:00:00+00:00",
        diagnostics=diagnostics,
    )

    assert [row["destination_iso2"] for row in rows] == ["CH", "DE", "FR"]
    assert rows[0]["published_at"] is None
    assert rows[1]["published_at"] == "2026-07-02"
    assert rows[2]["native_level"] is None
    assert rows[2]["published_at"] == "2026-07-03"
    assert diagnostics.blocking_skipped_rows == 1


def test_fetch_ca_advisories_rejects_invalid_payload_shape():
    with pytest.raises(ValueError, match="JSON object"):
        fetch_ca_advisories(fetch_json=lambda _url: [])


def test_parse_ca_index_reports_invalid_collection():
    diagnostics = BatchDiagnostics()
    assert parse_ca_index({"data": []}, diagnostics=diagnostics) == []
    assert diagnostics.blocking_skipped_rows == 1
    assert parse_ca_index({}) == []
