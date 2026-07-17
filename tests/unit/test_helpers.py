import pytest

from travelcanary_pipeline.ingestion.common.advisory import parse_us_level
from travelcanary_pipeline.resources.outbound_url import (
    validate_outbound_http_url,
    validate_outbound_https_url,
)


def test_parse_us_level_missing():
    assert parse_us_level("No level here") == (None, None)


def test_validate_outbound_urls():
    assert validate_outbound_https_url("https://example.com/path").startswith(
        "https://"
    )
    assert validate_outbound_http_url("http://data.gdeltproject.org/x").startswith(
        "http://"
    )


def test_validate_outbound_rejects_invalid():
    with pytest.raises(ValueError):
        validate_outbound_https_url("ftp://bad")
