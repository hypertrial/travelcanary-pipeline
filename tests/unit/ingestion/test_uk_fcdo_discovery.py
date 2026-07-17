import pytest
import requests

from travelcanary_pipeline.ingestion.uk_fcdo.advisories import (
    UK_CONTENT_API_BASE,
    discover_uk_country_slugs,
    fetch_uk_advisories,
)


def _sample_payload(title: str, slug: str) -> dict:
    return {
        "title": title,
        "description": "desc",
        "public_updated_at": "2026-07-01",
        "details": {
            "alert_status": ["see_travel_advice"],
            "country": {"name": title, "slug": slug},
        },
    }


def test_discover_uk_country_slugs_captures_country_names():
    payload = {
        "links": {
            "children": [
                {
                    "api_path": "/api/content/foreign-travel-advice/kuwait",
                    "details": {
                        "country": {"name": "Kuwait", "slug": "kuwait"},
                    },
                },
                {
                    "api_path": "/api/content/foreign-travel-advice/kosovo",
                    "details": {
                        "country": {"name": "Kosovo", "slug": "kosovo"},
                    },
                },
            ]
        }
    }

    slugs = discover_uk_country_slugs(fetch_json=lambda _url: payload)

    assert slugs == [
        {"slug": "kuwait", "iso2": "", "iso3": "", "name": "Kuwait"},
        {"slug": "kosovo", "iso2": "", "iso3": "", "name": "Kosovo"},
    ]


def test_fetch_uk_advisories_discover_all_resolves_non_seeded_country():
    index_payload = {
        "links": {
            "children": [
                {
                    "api_path": "/api/content/foreign-travel-advice/kuwait",
                    "details": {
                        "country": {"name": "Kuwait", "slug": "kuwait"},
                    },
                },
                {
                    "api_path": "/api/content/foreign-travel-advice/kosovo",
                    "details": {
                        "country": {"name": "Kosovo", "slug": "kosovo"},
                    },
                },
            ]
        }
    }

    def fake_fetch(url: str):
        if url == UK_CONTENT_API_BASE:
            return index_payload
        if url.endswith("/kuwait"):
            return _sample_payload("Kuwait", "kuwait")
        if url.endswith("/kosovo"):
            return _sample_payload("Kosovo", "kosovo")
        raise AssertionError(f"unexpected url: {url}")

    rows = fetch_uk_advisories(fetch_json=fake_fetch, discover_all=True)
    by_slug = {row["destination_native_id"]: row for row in rows}

    assert by_slug["kuwait"]["destination_iso2"] == "KW"
    assert by_slug["kuwait"]["destination_iso3"] == "KWT"
    assert by_slug["kosovo"]["destination_iso2"] is None
    assert by_slug["kosovo"]["destination_iso3"] is None


def test_fetch_uk_advisories_discover_all_prefers_seed_metadata():
    index_payload = {
        "links": {
            "children": [
                {
                    "api_path": "/api/content/foreign-travel-advice/thailand",
                    "details": {
                        "country": {"name": "Thailand", "slug": "thailand"},
                    },
                },
            ]
        }
    }

    def fake_fetch(url: str):
        if url == UK_CONTENT_API_BASE:
            return index_payload
        if url.endswith("/thailand"):
            return _sample_payload("Thailand", "thailand")
        raise AssertionError(f"unexpected url: {url}")

    rows = fetch_uk_advisories(fetch_json=fake_fetch, discover_all=True)

    assert len(rows) == 1
    assert rows[0]["destination_iso2"] == "TH"
    assert rows[0]["destination_iso3"] == "THA"


def test_fetch_uk_advisories_discover_all_skips_missing_slugs():
    index_payload = {
        "links": {
            "children": [
                {
                    "api_path": "/api/content/foreign-travel-advice/missing-country",
                    "details": {
                        "country": {"name": "Missing", "slug": "missing-country"},
                    },
                },
                {
                    "api_path": "/api/content/foreign-travel-advice/thailand",
                    "details": {
                        "country": {"name": "Thailand", "slug": "thailand"},
                    },
                },
            ]
        }
    }

    def fake_fetch(url: str):
        if url == UK_CONTENT_API_BASE:
            return index_payload
        if url.endswith("/missing-country"):
            response = requests.Response()
            response.status_code = 404
            raise requests.HTTPError(response=response)
        if url.endswith("/thailand"):
            return _sample_payload("Thailand", "thailand")
        raise AssertionError(f"unexpected url: {url}")

    rows = fetch_uk_advisories(fetch_json=fake_fetch, discover_all=True)

    assert len(rows) == 1
    assert rows[0]["destination_native_id"] == "thailand"


def test_discover_uk_country_slugs_rejects_invalid_index_and_uses_api_path_slug():
    from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics

    diagnostics = BatchDiagnostics()
    assert (
        discover_uk_country_slugs(fetch_json=lambda _url: [], diagnostics=diagnostics)
        == []
    )
    assert diagnostics.blocking_skipped_rows == 1

    payload = {
        "links": {
            "children": [
                "bad",
                {
                    "api_path": "/api/content/foreign-travel-advice/fiji",
                    "details": {"country": {"name": "Fiji"}},
                },
                {"api_path": "/elsewhere", "details": {"country": {"name": "Skip"}}},
            ]
        }
    }

    assert discover_uk_country_slugs(fetch_json=lambda _url: payload) == [
        {"slug": "fiji", "iso2": "", "iso3": "", "name": "Fiji"}
    ]


def test_uk_discovery_reports_invalid_links_and_children():
    from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics

    assert discover_uk_country_slugs(fetch_json=lambda _url: {}) == []
    assert (
        discover_uk_country_slugs(
            fetch_json=lambda _url: {
                "links": {"children": [{"api_path": "/elsewhere"}]}
            }
        )
        == []
    )

    links_diagnostics = BatchDiagnostics()
    assert (
        discover_uk_country_slugs(
            fetch_json=lambda _url: {"links": "bad"},
            diagnostics=links_diagnostics,
        )
        == []
    )
    assert links_diagnostics.skip_reasons == {"invalid index page": 1}

    children_diagnostics = BatchDiagnostics()
    assert (
        discover_uk_country_slugs(
            fetch_json=lambda _url: {"links": {"children": "bad"}},
            diagnostics=children_diagnostics,
        )
        == []
    )
    assert children_diagnostics.skip_reasons == {"invalid index child collection": 1}

    child_diagnostics = BatchDiagnostics()
    assert (
        discover_uk_country_slugs(
            fetch_json=lambda _url: {
                "links": {
                    "children": [
                        {"details": []},
                        {"details": {"country": []}},
                    ]
                }
            },
            diagnostics=child_diagnostics,
        )
        == []
    )
    assert child_diagnostics.skip_reasons == {"invalid index child": 2}


def test_fetch_uk_advisories_seed_default_invalid_payload_and_all_skipped():
    from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics

    def fake_seed():
        return [{"slug": "missing", "iso2": "", "iso3": "", "name": "Missing"}]

    response = requests.Response()
    response.status_code = 404

    diagnostics = BatchDiagnostics()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "travelcanary_pipeline.ingestion.uk_fcdo.advisories._load_slug_seed",
            fake_seed,
        )
        assert (
            fetch_uk_advisories(
                fetch_json=lambda _url: (_ for _ in ()).throw(
                    requests.HTTPError(response=response)
                ),
                diagnostics=diagnostics,
            )
            == []
        )
    assert diagnostics.skipped_rows == 1
    assert diagnostics.blocking_skipped_rows == 0

    diagnostics = BatchDiagnostics()
    assert (
        fetch_uk_advisories(
            slugs=[{"slug": "bad", "iso2": "", "iso3": "", "name": ""}],
            fetch_json=lambda _url: [],
            diagnostics=diagnostics,
        )
        == []
    )
    assert diagnostics.blocking_skipped_rows == 1


def test_fetch_uk_advisories_reports_invalid_entry_and_details():
    from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics

    diagnostics = BatchDiagnostics()
    rows = fetch_uk_advisories(
        slugs=[{}, {"slug": "bad", "iso2": "", "iso3": "", "name": ""}],
        fetch_json=lambda _url: {"details": []},
        diagnostics=diagnostics,
    )

    assert rows == []
    assert diagnostics.skip_reasons == {
        "invalid advisory details": 1,
        "invalid advisory index entry": 1,
    }


def test_fetch_uk_advisories_reraises_non_404_http_error():
    response = requests.Response()
    response.status_code = 500

    with pytest.raises(requests.HTTPError):
        fetch_uk_advisories(
            slugs=[{"slug": "bad", "iso2": "", "iso3": "", "name": ""}],
            fetch_json=lambda _url: (_ for _ in ()).throw(
                requests.HTTPError(response=response)
            ),
        )
