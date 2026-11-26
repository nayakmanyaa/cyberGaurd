"""CSV discovery and schema-enforced PySpark DataFrame loader."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.schema import CLEAN_COLUMN_NAMES, TRAFFIC_LABELLING_SCHEMA

logger = logging.getLogger(__name__)


def discover_traffic_csvs(
    raw_dir: Path,
    prefer_subdir: str = "TrafficLabelling",
) -> list[Path]:
    """Find TrafficLabelling CSV files under `raw_dir`.

    Prefers `raw_dir / prefer_subdir` when present so MachineLearningCVE files
    (which lack IPs/timestamps) are not double-counted alongside TrafficLabelling.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory does not exist: {raw_dir}")

    preferred = raw_dir / prefer_subdir
    search_root = preferred if preferred.exists() else raw_dir
    csv_files = sorted(p for p in search_root.rglob("*.csv") if p.is_file())
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under: {search_root}")

    logger.info("Discovered %d CSV file(s) under %s", len(csv_files), search_root)
    return csv_files


def read_traffic_csvs(
    spark: SparkSession,
    csv_paths: Sequence[Path],
    encoding: str = "ISO-8859-1",
    limit_per_file: int | None = None,
) -> DataFrame:
    """Read and union TrafficLabelling CSVs using the explicit Spark StructType schema."""
    if not csv_paths:
        raise ValueError("csv_paths must not be empty")

    expected_cols = len(CLEAN_COLUMN_NAMES)
    frames: list[DataFrame] = []

    for csv_path in csv_paths:
        # Read header line first via Spark to verify column count before applying positional schema.
        probe_df = (
            spark.read.option("header", "true")
            .option("encoding", encoding)
            .csv(str(csv_path))
        )
        if len(probe_df.columns) != expected_cols:
            raise ValueError(
                f"Schema mismatch in {csv_path.name}: expected {expected_cols} columns, "
                f"found {len(probe_df.columns)}"
            )

        df = (
            spark.read.option("header", "true")
            .option("encoding", encoding)
            .option("mode", "PERMISSIVE")
            .option("nullValue", "")
            .schema(TRAFFIC_LABELLING_SCHEMA)
            .csv(str(csv_path))
        )

        if limit_per_file and limit_per_file > 0:
            df = df.limit(limit_per_file)

        frames.append(df.withColumn("source_file", F.lit(csv_path.name)))
        logger.debug("Queued CSV %s for union", csv_path.name)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)

    return combined
