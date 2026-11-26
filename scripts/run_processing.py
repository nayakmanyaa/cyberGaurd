"""CLI runner for the CyberGuard PySpark DataFrame processing pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from processing.pipeline import run_processing_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Run CyberGuard PySpark data processing pipeline.")
    parser.add_argument("--raw-dir", type=Path, default=None, help="Override raw input directory.")
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=None,
        help="Override processed Parquet output directory.",
    )
    parser.add_argument(
        "--limit-per-file",
        type=int,
        default=None,
        help="Optional row limit per CSV file for fast sample testing.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    summary = run_processing_pipeline(
        raw_dir=args.raw_dir,
        processed_dir=args.processed_dir,
        limit_per_file=args.limit_per_file,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
