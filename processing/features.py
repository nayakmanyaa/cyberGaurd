"""PySpark DataFrame feature engineering supported by CIC-IDS2017 TrafficLabelling."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def engineer_flow_features(df: DataFrame) -> DataFrame:
    """Derive explainable per-flow traffic features using DataFrame transformations only.

    Note: `label` and `is_attack_label` are kept strictly for evaluation in later
    phases and must NOT be passed into threat detection rules as input features.
    """
    total_packets = F.col("total_fwd_packets") + F.col("total_bwd_packets")
    total_bytes = F.col("total_fwd_bytes") + F.col("total_bwd_bytes")
    duration_seconds = F.col("flow_duration") / F.lit(1_000_000.0)

    is_failed_or_reset = (F.col("rst_flag_count") > 0) | (
        (F.col("syn_flag_count") > 0) & (F.col("total_bwd_packets") == 0)
    )

    result = (
        df.withColumn("total_packets", total_packets)
        .withColumn("total_bytes", total_bytes)
        .withColumn(
            "computed_avg_packet_size",
            F.when(F.col("total_packets") > 0, F.col("total_bytes") / F.col("total_packets")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "outbound_inbound_byte_ratio",
            F.col("total_fwd_bytes") / (F.col("total_bwd_bytes") + F.lit(1.0)),
        )
        .withColumn(
            "outbound_inbound_pkt_ratio",
            F.col("total_fwd_packets") / (F.col("total_bwd_packets") + F.lit(1.0)),
        )
        .withColumn(
            "computed_bytes_per_sec",
            F.when(duration_seconds > 0, F.col("total_bytes") / duration_seconds).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "computed_packets_per_sec",
            F.when(duration_seconds > 0, F.col("total_packets") / duration_seconds).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "syn_ack_ratio",
            F.col("syn_flag_count").cast("double")
            / (F.col("ack_flag_count").cast("double") + F.lit(1.0)),
        )
        .withColumn("is_failed_or_reset", is_failed_or_reset)
    )

    if "label" in result.columns:
        result = result.withColumn(
            "is_attack_label",
            F.when(F.upper(F.col("label")) != F.lit("BENIGN"), F.lit(True)).otherwise(F.lit(False)),
        )

    return result
