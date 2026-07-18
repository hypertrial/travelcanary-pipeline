"""Export every public TravelCanary mart to Parquet."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from travelcanary_pipeline.config.settings import resolve_export_dir
from travelcanary_pipeline.export import ExportError, export_public_marts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for Parquet files and manifest.json (default: EXPORT_DIR).",
    )
    args = parser.parse_args(argv)
    output_dir = args.output_dir or resolve_export_dir()
    try:
        manifest = export_public_marts(output_dir)
    except ExportError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
