from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from .pipeline import run_pipeline


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
    print(asdict(result))


if __name__ == "__main__":
    main()
