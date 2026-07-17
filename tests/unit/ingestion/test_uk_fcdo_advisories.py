from travelcanary_pipeline.ingestion.uk_fcdo.advisories import parse_uk_travel_advice


def test_parse_uk_travel_advice_alert_status():
    row = parse_uk_travel_advice(
        "thailand",
        {
            "title": "Thailand",
            "description": "desc",
            "public_updated_at": "2026-07-01",
            "details": {
                "alert_status": ["avoid_all_but_essential_travel_to_the_whole_country"],
                "country": {"name": "Thailand"},
            },
        },
        iso2="TH",
        iso3="THA",
        ingested_at="2026-07-01T00:00:00+00:00",
    )
    assert row["native_level"] == "avoid_all_but_essential_travel_to_the_whole_country"
    assert row["destination_iso3"] == "THA"
