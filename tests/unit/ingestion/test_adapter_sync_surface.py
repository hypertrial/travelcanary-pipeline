from unittest.mock import patch

import pytest

from travelcanary_pipeline.ingestion.ca_gac.advisories import (
    fetch_ca_advisories,
    sync_ca_gac_advisories,
)
from travelcanary_pipeline.ingestion.jp_mofa.advisories import (
    fetch_jp_advisories,
    sync_jp_mofa_advisories,
)
from travelcanary_pipeline.ingestion.nl_mfa.advisories import (
    fetch_nl_advisories,
    sync_nl_mfa_advisories,
)
from travelcanary_pipeline.ingestion.uk_fcdo.advisories import (
    fetch_uk_advisories,
    sync_uk_fcdo_advisories,
)
from travelcanary_pipeline.ingestion.us_state.advisories import (
    fetch_us_advisories,
    parse_us_advisories,
)

INGESTED = "2026-07-01T00:00:00+00:00"
CA = {"data": {"TH": {"country-eng": "Thailand", "advisory-state": 2}}}
NL = "<documents><document><id>THA</id><introduction>geel</introduction></document></documents>"
JP = "<opendata><riskLevel1>1</riskLevel1></opendata>"
UK = {"details": {"alert_status": ["see_travel_advice"]}, "title": "Thailand"}


@pytest.mark.parametrize(
    ("sync", "kwargs"),
    [
        (sync_ca_gac_advisories, {"fetch_json": lambda _url: CA}),
        (sync_nl_mfa_advisories, {"fetch_text": lambda _url: NL}),
        (
            sync_jp_mofa_advisories,
            {
                "country_codes": [{"mofa_code": "0066", "iso3": "THA"}],
                "fetch_text": lambda _url: JP,
            },
        ),
        (
            sync_uk_fcdo_advisories,
            {
                "slugs": [{"slug": "thailand", "iso3": "THA"}],
                "fetch_json": lambda _url: UK,
            },
        ),
    ],
)
def test_adapter_sync_surfaces_use_the_shared_load(sync, kwargs):
    with patch(
        "travelcanary_pipeline.ingestion.common.dlt_runner.run_advisory_load"
    ) as load:
        result = sync(enforce_contract=False, **kwargs)
    assert result["rows"] == 1
    load.assert_called_once()


def test_fetch_surfaces_validate_payload_shapes():
    assert len(fetch_ca_advisories(fetch_json=lambda _url: CA)) == 1
    assert len(fetch_nl_advisories(fetch_text=lambda _url: NL)) == 1
    assert (
        len(
            fetch_jp_advisories(
                country_codes=[{"mofa_code": "0066", "iso3": "THA"}],
                fetch_text=lambda _url: JP,
            )
        )
        == 1
    )
    assert (
        len(
            fetch_uk_advisories(
                slugs=[{"slug": "thailand", "iso3": "THA"}],
                fetch_json=lambda _url: UK,
            )
        )
        == 1
    )

    with pytest.raises(ValueError):
        fetch_us_advisories(fetch_json=lambda _url: {})
    with pytest.raises(ValueError):
        fetch_ca_advisories(fetch_json=lambda _url: [])


def test_us_parser_chooses_valid_country_category():
    iso_rows = parse_us_advisories(
        [
            {
                "Title": "Thailand - Level 2: Exercise Increased Caution",
                "Category": ["Travel Advisory", "TH"],
            }
        ],
        ingested_at=INGESTED,
    )
    assert iso_rows[0]["destination_iso2"] == "TH"
    assert iso_rows[0]["destination_iso3"] == "THA"
    assert iso_rows[0]["destination_native_id"] == "TH"

    fips_rows = parse_us_advisories(
        [
            {
                "Title": "Kuwait - Level 3: Reconsider Travel",
                "Category": ["KU"],
            }
        ],
        ingested_at=INGESTED,
    )
    assert fips_rows[0]["destination_iso2"] == "KW"
    assert fips_rows[0]["destination_iso3"] == "KWT"

    override_rows = parse_us_advisories(
        [{"Title": "Hong Kong - Level 2: Exercise Increased Caution", "Category": []}],
        ingested_at=INGESTED,
    )
    assert override_rows[0]["destination_iso2"] == "HK"
    assert override_rows[0]["destination_iso3"] == "HKG"
