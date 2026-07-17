from unittest.mock import patch

from travelcanary_pipeline.ingestion.us_state.advisories import sync_us_state_advisories


def test_sync_us_state_advisories_uses_dlt_load():
    payload = [
        {
            "Title": "Thailand - Level 3: Reconsider Travel",
            "Category": ["TH"],
            "Summary": "sample",
            "Link": "https://example.com",
            "Updated": "2026-07-01",
        }
    ]
    with patch(
        "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
    ) as load:
        summary = sync_us_state_advisories(
            fetch_json=lambda _url: payload, enforce_contract=False
        )
    assert summary["rows"] == 1
    load.assert_called_once()
