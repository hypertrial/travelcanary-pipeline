"""Source quality contracts shared by ingestion and dbt."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

import pycountry

from travelcanary_pipeline.config.settings import BASE_DIR
from travelcanary_pipeline.naming import (
    ALL_ADVISORY_SOURCES,
    SOURCE_GDELT,
)


@dataclass(frozen=True)
class SourceContract:
    source: str
    role: str
    minimum_rows: int
    warn_drop_ratio: float
    reject_drop_ratio: float
    warn_after_hours: int
    error_after_hours: int
    minimum_canonical_ratio: float
    warn_canonical_ratio: float
    minimum_normalization_ratio: float

    @property
    def required(self) -> bool:
        return self.role == "required"


def _number(row: dict[str, str], field: str, number_type: type[int] | type[float]):
    try:
        return number_type(row[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"source_contracts.csv source={row.get('source')} field={field} "
            f"has invalid value {row.get(field)!r}"
        ) from exc


def _validate_contracts(contracts: list[SourceContract]) -> None:
    expected = {*ALL_ADVISORY_SOURCES, SOURCE_GDELT}
    sources = [contract.source for contract in contracts]
    duplicates = sorted({source for source in sources if sources.count(source) > 1})
    if duplicates:
        raise ValueError(
            "source_contracts.csv duplicate source field=source "
            f"value(s)={duplicates!r}"
        )
    missing = sorted(expected - set(sources))
    extra = sorted(set(sources) - expected)
    if missing or extra:
        raise ValueError(
            "source_contracts.csv field=source source set mismatch; "
            f"missing={missing or 'none'} extra={extra or 'none'}"
        )
    for contract in contracts:
        if contract.role != "required":
            raise ValueError(
                f"source_contracts.csv source={contract.source} field=role "
                f"must be 'required', got {contract.role!r}"
            )
        for field in ("minimum_rows", "warn_after_hours", "error_after_hours"):
            if getattr(contract, field) <= 0:
                value = getattr(contract, field)
                raise ValueError(
                    f"source_contracts.csv source={contract.source} field={field} "
                    f"has invalid value {value!r}; must be positive"
                )
        for field in (
            "warn_drop_ratio",
            "reject_drop_ratio",
            "minimum_canonical_ratio",
            "warn_canonical_ratio",
            "minimum_normalization_ratio",
        ):
            if not 0 <= getattr(contract, field) <= 1:
                value = getattr(contract, field)
                raise ValueError(
                    f"source_contracts.csv source={contract.source} field={field} "
                    f"has invalid value {value!r}; must be between 0 and 1"
                )
        if contract.reject_drop_ratio > contract.warn_drop_ratio:
            raise ValueError(
                f"source_contracts.csv source={contract.source} "
                f"field=reject_drop_ratio has invalid value "
                f"{contract.reject_drop_ratio!r}; must not exceed "
                f"warn_drop_ratio={contract.warn_drop_ratio!r}"
            )
        if contract.minimum_canonical_ratio > contract.warn_canonical_ratio:
            raise ValueError(
                f"source_contracts.csv source={contract.source} "
                f"field=minimum_canonical_ratio has invalid value "
                f"{contract.minimum_canonical_ratio!r}; must not exceed "
                f"warn_canonical_ratio={contract.warn_canonical_ratio!r}"
            )
        if contract.warn_after_hours >= contract.error_after_hours:
            raise ValueError(
                f"source_contracts.csv source={contract.source} "
                f"field=warn_after_hours has invalid value "
                f"{contract.warn_after_hours!r}; must be less than "
                f"error_after_hours={contract.error_after_hours!r}"
            )


def load_source_contracts(path: Path | None = None) -> dict[str, SourceContract]:
    path = path or BASE_DIR / "dbt" / "seeds" / "source_contracts.csv"
    with path.open(encoding="utf-8") as handle:
        contracts = [
            SourceContract(
                source=row["source"],
                role=row["role"],
                minimum_rows=_number(row, "minimum_rows", int),
                warn_drop_ratio=_number(row, "warn_drop_ratio", float),
                reject_drop_ratio=_number(row, "reject_drop_ratio", float),
                warn_after_hours=_number(row, "warn_after_hours", int),
                error_after_hours=_number(row, "error_after_hours", int),
                minimum_canonical_ratio=_number(row, "minimum_canonical_ratio", float),
                warn_canonical_ratio=_number(row, "warn_canonical_ratio", float),
                minimum_normalization_ratio=_number(
                    row, "minimum_normalization_ratio", float
                ),
            )
            for row in csv.DictReader(handle)
        ]
    _validate_contracts(contracts)
    return {contract.source: contract for contract in contracts}


def canonical_country_ratio(rows: list[dict[str, object]]) -> float:
    if not rows:
        return 0.0
    resolved = 0
    for row in rows:
        iso3 = str(row.get("destination_iso3") or "").upper()
        iso2 = str(row.get("destination_iso2") or "").upper()
        if (iso3 and pycountry.countries.get(alpha_3=iso3)) or (
            iso2 and pycountry.countries.get(alpha_2=iso2)
        ):
            resolved += 1
    return resolved / len(rows)


def normalization_ratio(source: str, rows: list[dict[str, object]]) -> float:
    path = BASE_DIR / "dbt" / "seeds" / "advisory_level_normalization.csv"
    with path.open(encoding="utf-8") as handle:
        mapped = {
            row["native_value"]
            for row in csv.DictReader(handle)
            if row["source"] == source
        }
    exceptions_path = (
        BASE_DIR / "dbt" / "seeds" / "advisory_normalization_exceptions.csv"
    )
    with exceptions_path.open(encoding="utf-8") as handle:
        exceptions = {
            row["native_value"]
            for row in csv.DictReader(handle)
            if row["source"] == source
        }
    native_values = [
        str(row["native_level"])
        for row in rows
        if row.get("native_level") is not None
        and str(row["native_level"]) not in exceptions
    ]
    if not native_values:
        return 1.0
    return sum(value in mapped for value in native_values) / len(native_values)


def duplicate_key_reason(rows: list[dict[str, object]], key: str) -> str | None:
    seen: set[object] = set()
    for row in rows:
        value = row.get(key)
        if value in seen:
            return f"duplicate {key}: {value}"
        seen.add(value)
    return None


__all__ = [
    "SourceContract",
    "canonical_country_ratio",
    "duplicate_key_reason",
    "load_source_contracts",
    "normalization_ratio",
]
