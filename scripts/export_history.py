"""Export country_travel_risk_history to a Parquet file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from travelcanary_pipeline.config.settings import resolve_export_dir
from travelcanary_pipeline.history_transfer import HistoryTransferError, export_history


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        type=Path,
        nargs="?",
        default=None,
        help=(
            "Destination Parquet path "
            "(default: EXPORT_DIR/country_travel_risk_history.parquet)."
        ),
    )
    args = parser.parse_args(argv)
    path = args.path or (resolve_export_dir() / "country_travel_risk_history.parquet")
    try:
        manifest = export_history(path)
    except HistoryTransferError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
