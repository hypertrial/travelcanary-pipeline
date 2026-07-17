"""Outbound URL validation."""

from __future__ import annotations

from urllib.parse import urlparse


def validate_outbound_https_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"only https URLs are allowed: {url}")
    if not parsed.netloc:
        raise ValueError(f"invalid URL: {url}")
    return url


def validate_outbound_http_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"only http(s) URLs are allowed: {url}")
    if not parsed.netloc:
        raise ValueError(f"invalid URL: {url}")
    return url


__all__ = ["validate_outbound_http_url", "validate_outbound_https_url"]
