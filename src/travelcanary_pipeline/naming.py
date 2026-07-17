"""Shared source naming helpers (no scope segment)."""

from __future__ import annotations

from dagster import AssetKey

SOURCE_US_STATE = "us_state"
SOURCE_CA_GAC = "ca_gac"
SOURCE_UK_FCDO = "uk_fcdo"
SOURCE_NL_MFA = "nl_mfa"
SOURCE_JP_MOFA = "jp_mofa"
SOURCE_GDELT = "gdelt"

ALL_ADVISORY_SOURCES = (
    SOURCE_US_STATE,
    SOURCE_CA_GAC,
    SOURCE_UK_FCDO,
    SOURCE_NL_MFA,
    SOURCE_JP_MOFA,
)


def flat_name(source: str, layer: str, *parts: str) -> str:
    return "_".join((source, layer, *parts))


def schema_name(source: str, layer: str) -> str:
    return flat_name(source, layer)


def asset_key(source: str, layer: str, *parts: str) -> AssetKey:
    return AssetKey([source, layer, *parts])


def raw_schema(source: str) -> str:
    return schema_name(source, "raw")


__all__ = [
    "ALL_ADVISORY_SOURCES",
    "SOURCE_CA_GAC",
    "SOURCE_GDELT",
    "SOURCE_JP_MOFA",
    "SOURCE_NL_MFA",
    "SOURCE_UK_FCDO",
    "SOURCE_US_STATE",
    "asset_key",
    "flat_name",
    "raw_schema",
    "schema_name",
]
