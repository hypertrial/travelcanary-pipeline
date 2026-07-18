"""Import country_travel_risk_history from a Parquet file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from travelcanary_pipeline.history_transfer import HistoryTransferError, import_history


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        type=Path,
        help="Source Parquet path previously written by export-history.",
    )
    args = parser.parse_args(argv)
    try:
        result = import_history(args.path)
    except HistoryTransferError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
