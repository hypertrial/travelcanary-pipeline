"""HTTP helpers with timeouts."""

from __future__ import annotations

from typing import BinaryIO

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from travelcanary_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from travelcanary_pipeline.resources.outbound_url import (
    validate_outbound_http_url,
    validate_outbound_https_url,
)

DEFAULT_HEADERS = {"User-Agent": "TravelCanary/0.5.0 (+https://github.com/hypertrial)"}
_RETRY = Retry(
    total=3,
    connect=3,
    read=0,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "HEAD"}),
)
_SESSION: requests.Session | None = None


def _http_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=_RETRY)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _SESSION = session
    return _SESSION


def get_json(url: str, *, verify: bool = True, timeout=HTTP_REQUEST_TIMEOUT) -> object:
    response = _http_session().get(
        validate_outbound_https_url(url),
        headers=DEFAULT_HEADERS,
        timeout=timeout,
        verify=verify,
    )
    response.raise_for_status()
    return response.json()


def get_text(url: str, *, verify: bool = True, timeout=HTTP_REQUEST_TIMEOUT) -> str:
    response = _http_session().get(
        validate_outbound_https_url(url),
        headers=DEFAULT_HEADERS,
        timeout=timeout,
        verify=verify,
    )
    response.raise_for_status()
    return response.text


def download_to_file(
    url: str,
    destination: BinaryIO,
    *,
    verify: bool = True,
    timeout=HTTP_REQUEST_TIMEOUT,
) -> None:
    response = _http_session().get(
        validate_outbound_http_url(url),
        headers=DEFAULT_HEADERS,
        timeout=timeout,
        verify=verify,
        stream=True,
    )
    try:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                destination.write(chunk)
    finally:
        response.close()


__all__ = ["DEFAULT_HEADERS", "download_to_file", "get_json", "get_text"]
