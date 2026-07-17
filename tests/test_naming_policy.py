from travelcanary_pipeline.naming import (
    SOURCE_US_STATE,
    asset_key,
    flat_name,
    raw_schema,
    schema_name,
)


def test_flat_name_has_no_scope_token():
    assert flat_name(SOURCE_US_STATE, "raw", "advisories") == "us_state_raw_advisories"


def test_schema_and_asset_key():
    assert schema_name(SOURCE_US_STATE, "raw") == "us_state_raw"
    assert asset_key(SOURCE_US_STATE, "raw", "advisories").path == [
        "us_state",
        "raw",
        "advisories",
    ]


def test_raw_schema_helper():
    assert raw_schema(SOURCE_US_STATE) == "us_state_raw"
