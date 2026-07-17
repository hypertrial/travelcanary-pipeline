"""Resolve GOV.UK country slugs/names to ISO-3166-1 alpha-2/alpha-3 codes."""

from __future__ import annotations

import re

import pycountry

# GOV.UK slugs that bundle multiple ISO territories or lack an ISO-3166-1 code.
# These remain unmapped and are excluded from marts (documented in docs/warehouse.md).
UNMAPPED_SLUGS: frozenset[str] = frozenset(
    {
        "kosovo",  # no ISO-3166-1 code assigned
        "cook-islands-tokelau-and-niue",  # multi-territory GOV.UK page
        "st-martin-and-st-barthelemy",  # multi-territory GOV.UK page
    }
)

# Individually verified aliases where pycountry lookup fails on the GOV.UK name.
SLUG_ISO_OVERRIDES: dict[str, tuple[str, str]] = {
    # Gambia
    "the-gambia": ("GM", "GMB"),
    # Türkiye
    "turkey": ("TR", "TUR"),
    # Russian Federation
    "russia": ("RU", "RUS"),
    # Congo, The Democratic Republic of the
    "democratic-republic-of-the-congo": ("CD", "COD"),
    # Palestine, State of
    "palestine": ("PS", "PSE"),
    # Cabo Verde
    "cape-verde": ("CV", "CPV"),
    # Brunei Darussalam
    "brunei": ("BN", "BRN"),
    # Falkland Islands (Malvinas)
    "falkland-islands": ("FK", "FLK"),
    # Sao Tome and Principe
    "sao-tome-and-principe": ("ST", "STP"),
    # Bonaire, Sint Eustatius and Saba
    "bonaire-st-eustatius-saba": ("BQ", "BES"),
    # Sint Maarten (Dutch part)
    "st-maarten": ("SX", "SXM"),
    # Antarctica
    "antarctica-british-antarctic-territory": ("AQ", "ATA"),
    # Pitcairn
    "pitcairn-island": ("PN", "PCN"),
}


def normalize_country_name(name: str) -> str:
    """Normalize GOV.UK country names for pycountry lookup."""
    normalized = re.sub(r"\s*\(.*?\)", "", name)
    normalized = normalized.replace("&", "and")
    normalized = re.sub(r"\bSt\b\.?", "Saint", normalized)
    normalized = re.sub(r"/", ", ", normalized)
    return normalized.strip()


def _lookup_iso(name: str) -> tuple[str | None, str | None]:
    try:
        match = pycountry.countries.lookup(name)
    except LookupError:
        return None, None
    return match.alpha_2, match.alpha_3


def resolve_iso(slug: str, name: str) -> tuple[str | None, str | None]:
    """Resolve a GOV.UK slug and country name to ISO2/ISO3 codes."""
    if slug in UNMAPPED_SLUGS:
        return None, None
    if slug in SLUG_ISO_OVERRIDES:
        return SLUG_ISO_OVERRIDES[slug]
    iso2, iso3 = _lookup_iso(name)
    if iso2 and iso3:
        return iso2, iso3
    return _lookup_iso(normalize_country_name(name))


__all__ = [
    "SLUG_ISO_OVERRIDES",
    "UNMAPPED_SLUGS",
    "normalize_country_name",
    "resolve_iso",
]
