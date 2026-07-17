import json
from pathlib import Path

import pytest

from travelcanary_pipeline.ingestion.uk_fcdo.iso_resolver import (
    SLUG_ISO_OVERRIDES,
    UNMAPPED_SLUGS,
    normalize_country_name,
    resolve_iso,
)

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "uk_fcdo_country_index_sample.json"
)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Myanmar (Burma)", "Myanmar"),
        ("St Vincent and the Grenadines", "Saint Vincent and the Grenadines"),
        ("St Pierre & Miquelon", "Saint Pierre and Miquelon"),
        ("Bonaire/St Eustatius/Saba", "Bonaire, Saint Eustatius, Saba"),
    ],
)
def test_normalize_country_name(name: str, expected: str):
    assert normalize_country_name(name) == expected


@pytest.mark.parametrize(
    ("slug", "name", "iso2", "iso3"),
    [
        ("france", "France", "FR", "FRA"),
        ("kuwait", "Kuwait", "KW", "KWT"),
        ("myanmar", "Myanmar (Burma)", "MM", "MMR"),
        ("st-lucia", "St Lucia", "LC", "LCA"),
    ],
)
def test_resolve_iso_exact_match(slug: str, name: str, iso2: str, iso3: str):
    assert resolve_iso(slug, name) == (iso2, iso3)


@pytest.mark.parametrize("slug", sorted(SLUG_ISO_OVERRIDES))
def test_resolve_iso_overrides(slug: str):
    iso2, iso3 = resolve_iso(slug, "ignored")
    assert iso2 == SLUG_ISO_OVERRIDES[slug][0]
    assert iso3 == SLUG_ISO_OVERRIDES[slug][1]


@pytest.mark.parametrize("slug", sorted(UNMAPPED_SLUGS))
def test_resolve_iso_unmapped_slugs(slug: str):
    assert resolve_iso(slug, "Any Name") == (None, None)


def test_resolve_iso_unknown_slug_returns_none():
    assert resolve_iso("not-a-real-country", "Atlantis") == (None, None)


def test_uk_fcdo_country_index_resolution_coverage():
    entries = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    unresolved = [
        entry["slug"]
        for entry in entries
        if resolve_iso(entry["slug"], entry["name"]) == (None, None)
    ]
    assert unresolved == sorted(UNMAPPED_SLUGS)
    resolved_count = len(entries) - len(unresolved)
    assert resolved_count / len(entries) >= 0.95
