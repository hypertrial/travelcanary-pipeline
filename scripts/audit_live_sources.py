"""Read-only live audit of every TravelCanary source contract."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone

from travelcanary_pipeline.live_audit import (
    audit_sources,
    has_blocking_required_failure,
    select_sources,
)


def _progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _format_text(results: list[dict[str, object]]) -> str:
    lines: list[str] = []
    detail_keys = (
        "discovered_rows",
        "skipped_rows",
        "blocking_skipped_rows",
        "canonical_ratio",
        "normalization_ratio",
        "mapped_geography_ratio",
    )
    for result in results:
        parts = [
            f"source={result['source']}",
            f"status={result['status']}",
            f"role={result['role']}",
            f"rows={result.get('rows', 0)}",
            f"minimum_rows={result.get('minimum_rows')}",
            f"duration_seconds={result.get('duration_seconds')}",
        ]
        for key in (
            "previous_context_status",
            "previous_context_reason",
            "previous_accepted_rows",
            "previous_accepted_finished_at",
            "relative_ratio",
        ):
            if result.get(key) is not None:
                parts.append(f"{key}={result[key]}")
        parts.extend(f"{key}={result[key]}" for key in detail_keys if key in result)
        if result.get("skip_reasons"):
            parts.append(
                f"skip_reasons={json.dumps(result['skip_reasons'], sort_keys=True)}"
            )
        if "proposed_minimum_rows" in result:
            parts.append(f"proposed_minimum_rows={result['proposed_minimum_rows']}")
        if result.get("source_url"):
            parts.append(f"source_url={result['source_url']}")
        if result.get("reason"):
            parts.append(f"reason={result['reason']}")
        lines.append(" ".join(parts))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        help="Source ID to audit; repeatable. Defaults to every source contract.",
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=datetime.now(timezone.utc).date() - timedelta(days=1),
        help="GDELT UTC export date (YYYY-MM-DD; default: yesterday)",
    )
    parser.add_argument(
        "--output",
        choices=("json", "text"),
        default="json",
        help="Audit output format; progress always goes to stderr.",
    )
    parser.add_argument(
        "--propose-floors",
        action="store_true",
        help="Add 80%% floor proposals for accepted live source counts.",
    )
    parser.add_argument(
        "--warehouse",
        help=(
            "Optional DuckDB warehouse path for read-only previous accepted run "
            "context. Defaults to no warehouse access."
        ),
    )
    args = parser.parse_args(argv)
    try:
        selected_sources = select_sources(args.sources)
    except ValueError as exc:
        parser.error(str(exc))
    results = audit_sources(
        args.date,
        selected_sources=selected_sources,
        propose_floors=args.propose_floors,
        warehouse=args.warehouse,
        progress=_progress,
    )
    if args.output == "json":
        print(json.dumps(results, indent=2, sort_keys=True))
    else:
        print(_format_text(results))
    return int(has_blocking_required_failure(results))


if __name__ == "__main__":
    raise SystemExit(main())
