"""PySpark DataFrame cleaning, typing, timestamp parsing, and deduplication."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from ingestion.schema import NUMERIC_DOUBLE_COLUMNS

logger = logging.getLogger(__name__)


def drop_duplicate_raw_columns(df: DataFrame) -> DataFrame:
    """Drop the duplicate `Fwd Header Length` column present in CIC-IDS2017 CSV headers."""
    if "fwd_header_length_dup" in df.columns:
        return df.drop("fwd_header_length_dup")
    return df


def trim_and_normalize_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace from string columns and map protocol codes to names."""
    result = df
    for col_name in ("flow_id", "source_ip", "destination_ip", "timestamp_raw", "label"):
        if col_name in result.columns:
            result = result.withColumn(col_name, F.trim(F.col(col_name)))

    if "label" in result.columns:
        result = result.withColumn(
            "label",
            F.regexp_replace(F.col("label"), r"[\u0096\u2013\ufffd]", "-"),
        )

    if "protocol" in result.columns:
        result = result.withColumn(
            "protocol_name",
            F.when(F.col("protocol") == 6, F.lit("TCP"))
            .when(F.col("protocol") == 17, F.lit("UDP"))
            .when(F.col("protocol") == 1, F.lit("ICMP"))
            .when(F.col("protocol") == 0, F.lit("HOPOPT"))
            .otherwise(F.lit("OTHER")),
        )
    return result


def parse_timestamps(df: DataFrame) -> DataFrame:
    """Parse CIC-IDS2017 timestamp strings into `event_timestamp`, `event_date`, and `time_window_5m`."""
    ts_col = F.col("timestamp_raw")
    parsed_ts = F.coalesce(
        F.expr("try_to_timestamp(timestamp_raw, 'dd/MM/yyyy HH:mm:ss')"),
        F.expr("try_to_timestamp(timestamp_raw, 'd/M/yyyy H:mm:ss')"),
        F.expr("try_to_timestamp(timestamp_raw, 'd/M/yyyy H:mm')"),
        F.expr("try_to_timestamp(timestamp_raw, 'dd/MM/yyyy HH:mm')"),
    )

    return (
        df.withColumn("event_timestamp", F.when(ts_col.isNotNull(), parsed_ts))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("time_window_5m", F.window("event_timestamp", "5 minutes").getField("start"))
    )


def _invalid_identity_expr(has_parsed_ts: bool) -> F.Column:
    cond = (
        F.col("source_ip").isNull()
        | (F.trim(F.col("source_ip")) == "")
        | F.col("destination_ip").isNull()
        | (F.trim(F.col("destination_ip")) == "")
        | F.col("source_port").isNull()
        | F.col("destination_port").isNull()
        | F.col("protocol").isNull()
        | F.col("timestamp_raw").isNull()
        | (F.trim(F.col("timestamp_raw")) == "")
    )
    if has_parsed_ts:
        cond = cond | F.col("event_timestamp").isNull()
    return cond


def count_quality_issues(df: DataFrame) -> dict[str, int]:
    """Compute total rows, invalid-identity rows, and non-finite/null numeric cells in one Spark pass."""
    double_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, DoubleType)]
    has_parsed_ts = "event_timestamp" in df.columns

    exprs = [
        F.count("*").alias("total_rows"),
        F.sum(F.when(_invalid_identity_expr(has_parsed_ts), 1).otherwise(0)).alias(
            "invalid_identity_rows"
        ),
    ]

    for idx, col_name in enumerate(double_cols):
        c = F.col(col_name)
        exprs.append(
            F.sum(
                F.when(
                    c.isNull()
                    | F.isnan(c)
                    | (c == F.lit(float("inf")))
                    | (c == F.lit(float("-inf"))),
                    1,
                ).otherwise(0)
            ).alias(f"bad_double_{idx}")
        )

    for idx, col_name in enumerate(("source_port", "destination_port", "protocol")):
        if col_name in df.columns:
            exprs.append(
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"null_int_{idx}")
            )

    row = df.agg(*exprs).collect()[0].asDict()
    total_rows = int(row.get("total_rows", 0) or 0)
    invalid_identity_rows = int(row.get("invalid_identity_rows", 0) or 0)
    non_finite_or_null_numeric_cells = sum(
        int(v or 0) for k, v in row.items() if k.startswith(("bad_double_", "null_int_"))
    )
    return {
        "total_rows": total_rows,
        "invalid_identity_rows": invalid_identity_rows,
        "non_finite_or_null_numeric_cells": non_finite_or_null_numeric_cells,
    }


def handle_missing_and_invalid(df: DataFrame) -> DataFrame:
    """Drop rows without valid identity/timestamp and sanitize NaN/Inf/negative durations."""
    filtered = df.filter(~_invalid_identity_expr(has_parsed_ts="event_timestamp" in df.columns))

    result = filtered
    active_double_cols = [c for c in NUMERIC_DOUBLE_COLUMNS if c in result.columns]
    for col_name in active_double_cols:
        c = F.col(col_name)
        sanitized = F.when(
            c.isNull()
            | F.isnan(c)
            | (c == F.lit(float("inf")))
            | (c == F.lit(float("-inf"))),
            F.lit(0.0),
        ).otherwise(c)
        result = result.withColumn(col_name, sanitized)

    if "flow_duration" in result.columns:
        result = result.withColumn(
            "flow_duration", F.greatest(F.col("flow_duration"), F.lit(0.0))
        )

    for int_col in (
        "fwd_psh_flags",
        "bwd_psh_flags",
        "fwd_urg_flags",
        "bwd_urg_flags",
        "fin_flag_count",
        "syn_flag_count",
        "rst_flag_count",
        "psh_flag_count",
        "ack_flag_count",
        "urg_flag_count",
        "cwe_flag_count",
        "ece_flag_count",
        "init_win_bytes_forward",
        "init_win_bytes_backward",
    ):
        if int_col in result.columns:
            result = result.withColumn(int_col, F.coalesce(F.col(int_col), F.lit(0)))

    if "label" in result.columns:
        result = result.withColumn("label", F.coalesce(F.col("label"), F.lit("UNKNOWN")))

    return result


def deduplicate_flows(df: DataFrame) -> DataFrame:
    """Remove exact duplicate flow records (ignoring provenance `source_file`)."""
    dedup_cols = [c for c in df.columns if c != "source_file"]
    return df.dropDuplicates(dedup_cols)
