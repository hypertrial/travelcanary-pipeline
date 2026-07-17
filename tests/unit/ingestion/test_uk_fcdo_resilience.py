import requests

from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.ingestion.uk_fcdo.advisories import fetch_uk_advisories


def test_fetch_uk_advisories_skips_missing_slugs():
    def fake_fetch(url: str):
        if url.endswith("/missing-country"):
            response = requests.Response()
            response.status_code = 404
            raise requests.HTTPError(response=response)
        return {
            "title": "Thailand",
            "description": "desc",
            "public_updated_at": "2026-07-01",
            "details": {
                "alert_status": ["see_travel_advice"],
                "country": {"name": "Thailand"},
            },
        }

    rows = fetch_uk_advisories(
        slugs=[
            {"slug": "missing-country", "iso2": "", "iso3": "", "name": ""},
            {"slug": "thailand", "iso2": "TH", "iso3": "THA", "name": "Thailand"},
        ],
        fetch_json=fake_fetch,
    )
    assert len(rows) == 1
    assert rows[0]["destination_native_id"] == "thailand"


def test_fetch_uk_advisories_allows_an_all_withdrawn_batch_for_contract_check():
    def fake_fetch(_url: str):
        response = requests.Response()
        response.status_code = 404
        raise requests.HTTPError(response=response)

    diagnostics = BatchDiagnostics()
    assert (
        fetch_uk_advisories(
            slugs=[{"slug": "missing-country", "iso2": "", "iso3": "", "name": ""}],
            fetch_json=fake_fetch,
            diagnostics=diagnostics,
        )
        == []
    )
    assert diagnostics.skip_reasons == {"withdrawn advisory (HTTP 404)": 1}
    assert diagnostics.blocking_skipped_rows == 0
