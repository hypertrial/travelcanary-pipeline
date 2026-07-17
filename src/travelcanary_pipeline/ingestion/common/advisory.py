"""Shared advisory row shape and helpers."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypedDict


class AdvisoryRow(TypedDict):
    source_run_id: str
    advisory_id: str
    source: str
    destination_native_id: str
    destination_iso2: str | None
    destination_iso3: str | None
    destination_name: str | None
    native_level: str | None
    native_level_label: str | None
    summary_text: str | None
    source_url: str | None
    published_at: str | None
    ingested_at: str


@dataclass
class BatchDiagnostics:
    discovered_rows: int = 0
    skipped_rows: int = 0
    blocking_skipped_rows: int = 0
    _reasons: Counter[str] = field(default_factory=Counter)

    def observe(self) -> None:
        self.discovered_rows += 1

    def skip(self, reason: str, *, blocking: bool = True) -> None:
        self.discovered_rows += 1
        self.skipped_rows += 1
        if blocking:
            self.blocking_skipped_rows += 1
        self._reasons[reason] += 1

    @property
    def skip_reasons(self) -> dict[str, int]:
        return dict(sorted(self._reasons.items()))

    def summary(self) -> dict[str, object]:
        return {
            "discovered_rows": self.discovered_rows,
            "skipped_rows": self.skipped_rows,
            "blocking_skipped_rows": self.blocking_skipped_rows,
            "skip_reasons": self.skip_reasons,
        }

    def message(self) -> str | None:
        if not self.skipped_rows:
            return None
        reasons = ", ".join(
            f"{reason}={count}" for reason, count in self.skip_reasons.items()
        )
        return (
            f"skipped_rows={self.skipped_rows}; "
            f"blocking_skipped_rows={self.blocking_skipped_rows}; {reasons}"
        )


_LEVEL_RE = re.compile(r"level\s*(\d)", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_advisory_id(source: str, destination_native_id: str) -> str:
    return f"{source}:{destination_native_id}"


def parse_us_level(title: str) -> tuple[str | None, str | None]:
    match = _LEVEL_RE.search(title or "")
    if not match:
        return None, None
    level = match.group(1)
    return level, f"Level {level}"


def row_from_parts(
    *,
    source: str,
    destination_native_id: str,
    destination_iso2: str | None = None,
    destination_iso3: str | None = None,
    destination_name: str | None = None,
    native_level: str | None = None,
    native_level_label: str | None = None,
    summary_text: str | None = None,
    source_url: str | None = None,
    published_at: str | None = None,
    ingested_at: str | None = None,
) -> AdvisoryRow:
    return AdvisoryRow(
        source_run_id="",
        advisory_id=make_advisory_id(source, destination_native_id),
        source=source,
        destination_native_id=destination_native_id,
        destination_iso2=destination_iso2,
        destination_iso3=destination_iso3,
        destination_name=destination_name,
        native_level=native_level,
        native_level_label=native_level_label,
        summary_text=summary_text,
        source_url=source_url,
        published_at=published_at,
        ingested_at=ingested_at or utc_now_iso(),
    )


ADVISORY_COLUMNS = {
    "source_run_id": {"data_type": "text"},
    "advisory_id": {"data_type": "text"},
    "source": {"data_type": "text"},
    "destination_native_id": {"data_type": "text"},
    "destination_iso2": {"data_type": "text", "nullable": True},
    "destination_iso3": {"data_type": "text", "nullable": True},
    "destination_name": {"data_type": "text", "nullable": True},
    "native_level": {"data_type": "text", "nullable": True},
    "native_level_label": {"data_type": "text", "nullable": True},
    "summary_text": {"data_type": "text", "nullable": True},
    "source_url": {"data_type": "text", "nullable": True},
    "published_at": {"data_type": "text", "nullable": True},
    "ingested_at": {"data_type": "text"},
}


def rows_to_dicts(rows: list[AdvisoryRow]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


__all__ = [
    "ADVISORY_COLUMNS",
    "AdvisoryRow",
    "BatchDiagnostics",
    "make_advisory_id",
    "parse_us_level",
    "row_from_parts",
    "rows_to_dicts",
    "utc_now_iso",
]
