from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path

from .pipeline import PipelineResult, run_pipeline


def format_result(result: PipelineResult) -> str:
    """Render a pipeline result as stable, machine-readable JSON."""
    return json.dumps(asdict(result), sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load product events into a local analytics warehouse.")
    parser.add_argument("--source", type=Path, required=True, help="CSV file containing product events")
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("data/analytics.db"),
        help="SQLite warehouse path (default: data/analytics.db)",
    )
    args = parser.parse_args()
    result = run_pipeline(args.source, args.database)
    print(format_result(result))


if __name__ == "__main__":
    main()
