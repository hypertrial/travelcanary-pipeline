from __future__ import annotations

import csv

from travelcanary_pipeline.config.settings import BASE_DIR
from travelcanary_pipeline.ingestion.us_state.advisories import _US_FIPS_OVERRIDES

FIPS_SEED = BASE_DIR / "dbt" / "seeds" / "fips10_4_to_iso3.csv"
ISO_SEED = BASE_DIR / "dbt" / "seeds" / "iso_countries.csv"


def _fips_map() -> dict[str, str]:
    with FIPS_SEED.open(encoding="utf-8") as handle:
        return {row["fips10_4"]: row["iso3"] for row in csv.DictReader(handle)}


def test_fips_seed_maps_guinea_and_equatorial_guinea_distinctly():
    fips = _fips_map()
    assert fips["GV"] == "GIN"
    assert fips["EK"] == "GNQ"


def test_fips_seed_includes_us_overrides_and_iso_targets():
    fips = _fips_map()
    for code, iso3 in _US_FIPS_OVERRIDES.items():
        assert fips.get(code) == iso3, code

    with ISO_SEED.open(encoding="utf-8") as handle:
        iso_countries = {row["iso3"] for row in csv.DictReader(handle)}
    for iso3 in ("GIN", "SVK", "SXM", "CUW"):
        assert iso3 in iso_countries
        assert iso3 in set(fips.values()), iso3
