"""Reusable end-to-end PySpark DataFrame processing pipeline."""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from config.settings import AppConfig, load_config
from config.spark_session import create_spark_session
from ingestion.csv_loader import discover_traffic_csvs, read_traffic_csvs
from processing.aggregations import (
    aggregate_by_destination_port,
    aggregate_by_ip_pair,
    aggregate_by_source_ip,
    aggregate_by_time_window,
)
from processing.cleaner import (
    count_quality_issues,
    deduplicate_flows,
    drop_duplicate_raw_columns,
    handle_missing_and_invalid,
    parse_timestamps,
    trim_and_normalize_strings,
)
from processing.features import engineer_flow_features

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessingSummary:
    """Metrics recorded during a batch PySpark processing run."""

    files_processed: int
    input_record_count: int
    rows_after_cleaning: int
    dropped_invalid_rows: int
    output_record_count: int
    duplicates_removed: int
    missing_identity_before: int
    non_finite_or_null_numeric_before: int
    missing_identity_after: int
    non_finite_or_null_numeric_after: int
    source_ip_agg_count: int
    ip_pair_agg_count: int
    time_window_agg_count: int
    dst_port_agg_count: int
    processing_duration_sec: float
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def transform_flows(raw_df: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    """Run cleaning, timestamp parsing, deduplication, and feature engineering."""
    df = drop_duplicate_raw_columns(raw_df)
    df = trim_and_normalize_strings(df)
    df = parse_timestamps(df)

    quality_before = count_quality_issues(df)
    input_count = quality_before["total_rows"]
    dropped_invalid_rows = quality_before["invalid_identity_rows"]
    rows_after_cleaning = input_count - dropped_invalid_rows

    df = handle_missing_and_invalid(df)
    df = deduplicate_flows(df)
    df = engineer_flow_features(df)

    metrics = {
        "input_record_count": input_count,
        "rows_after_cleaning": rows_after_cleaning,
        "dropped_invalid_rows": dropped_invalid_rows,
        "missing_identity_before": quality_before["invalid_identity_rows"],
        "non_finite_or_null_numeric_before": quality_before["non_finite_or_null_numeric_cells"],
    }
    return df, metrics


def run_processing_pipeline(
    config: AppConfig | None = None,
    raw_dir: Path | None = None,
    processed_dir: Path | None = None,
    limit_per_file: int | None = None,
    spark: SparkSession | None = None,
) -> ProcessingSummary:
    """Execute the batch PySpark DataFrame pipeline and save Parquet outputs."""
    start = time.perf_counter()
    cfg = config or load_config()
    target_raw = raw_dir or cfg.paths.raw_dir
    target_out = processed_dir or cfg.paths.processed_dir
    target_out.mkdir(parents=True, exist_ok=True)

    owns_spark = spark is None
    active_spark = spark or create_spark_session(cfg.spark)

    try:
        csv_paths = discover_traffic_csvs(target_raw)
        logger.info("Starting PySpark processing for %d CSV file(s)", len(csv_paths))

        raw_df = read_traffic_csvs(active_spark, csv_paths, limit_per_file=limit_per_file)
        cleaned_df, clean_metrics = transform_flows(raw_df)

        flows_path = target_out / "cleaned_flows.parquet"
        src_ip_path = target_out / "agg_source_ip.parquet"
        ip_pair_path = target_out / "agg_ip_pairs.parquet"
        window_path = target_out / "agg_time_windows.parquet"
        port_path = target_out / "agg_destination_ports.parquet"

        logger.info("Writing cleaned flows Parquet to %s", flows_path)
        cleaned_df.write.mode("overwrite").parquet(str(flows_path))

        # Read from persisted columnar Parquet for post-write verification and aggregations.
        persisted_flows = active_spark.read.parquet(str(flows_path))
        quality_after = count_quality_issues(persisted_flows)
        output_count = quality_after["total_rows"]
        duplicates_removed = clean_metrics["rows_after_cleaning"] - output_count

        logger.info("Computing and writing aggregated Parquet tables under %s", target_out)
        aggregate_by_source_ip(persisted_flows).write.mode("overwrite").parquet(str(src_ip_path))
        aggregate_by_ip_pair(persisted_flows).write.mode("overwrite").parquet(str(ip_pair_path))
        aggregate_by_time_window(persisted_flows).write.mode("overwrite").parquet(str(window_path))
        aggregate_by_destination_port(persisted_flows).write.mode("overwrite").parquet(str(port_path))

        src_ip_count = active_spark.read.parquet(str(src_ip_path)).count()
        ip_pair_count = active_spark.read.parquet(str(ip_pair_path)).count()
        window_count = active_spark.read.parquet(str(window_path)).count()
        port_count = active_spark.read.parquet(str(port_path)).count()

        duration = round(time.perf_counter() - start, 2)
        summary = ProcessingSummary(
            files_processed=len(csv_paths),
            input_record_count=clean_metrics["input_record_count"],
            rows_after_cleaning=clean_metrics["rows_after_cleaning"],
            dropped_invalid_rows=clean_metrics["dropped_invalid_rows"],
            output_record_count=output_count,
            duplicates_removed=duplicates_removed,
            missing_identity_before=clean_metrics["missing_identity_before"],
            non_finite_or_null_numeric_before=clean_metrics["non_finite_or_null_numeric_before"],
            missing_identity_after=quality_after["invalid_identity_rows"],
            non_finite_or_null_numeric_after=quality_after["non_finite_or_null_numeric_cells"],
            source_ip_agg_count=src_ip_count,
            ip_pair_agg_count=ip_pair_count,
            time_window_agg_count=window_count,
            dst_port_agg_count=port_count,
            processing_duration_sec=duration,
            output_dir=str(target_out),
        )
        logger.info("Processing pipeline completed in %.2fs", duration)
        return summary
    except Exception:
        logger.exception("PySpark processing pipeline failed")
        raise
    finally:
        if owns_spark:
            active_spark.stop()
