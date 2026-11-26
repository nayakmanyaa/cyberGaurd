"""Distributed PySpark DataFrame aggregations for traffic summarization."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_by_source_ip(df: DataFrame) -> DataFrame:
    """Aggregate network traffic behavior per source IP address."""
    return df.groupBy("source_ip").agg(
        F.count("*").alias("connection_frequency"),
        F.countDistinct("destination_ip").alias("distinct_dst_ips"),
        F.countDistinct("destination_port").alias("distinct_dst_ports"),
        F.sum("total_bytes").alias("total_bytes_transferred"),
        F.sum("total_fwd_bytes").alias("total_fwd_bytes"),
        F.sum("total_bwd_bytes").alias("total_bwd_bytes"),
        F.sum("total_packets").alias("total_packets"),
        F.avg("computed_avg_packet_size").alias("avg_packet_size"),
        F.avg("flow_duration").alias("avg_flow_duration"),
        F.sum(F.when(F.col("is_failed_or_reset"), 1).otherwise(0)).alias("failed_or_reset_count"),
        F.sum("syn_flag_count").alias("total_syn_flags"),
        F.sum("rst_flag_count").alias("total_rst_flags"),
        F.sort_array(F.collect_set("protocol_name")).alias("protocols_used"),
        F.min("event_timestamp").alias("first_seen"),
        F.max("event_timestamp").alias("last_seen"),
    )


def aggregate_by_ip_pair(df: DataFrame) -> DataFrame:
    """Aggregate directed IP-to-IP communication edges (foundation for Neo4j graph)."""
    return df.groupBy("source_ip", "destination_ip").agg(
        F.count("*").alias("connection_frequency"),
        F.countDistinct("destination_port").alias("distinct_dst_ports"),
        F.sum("total_bytes").alias("total_bytes_transferred"),
        F.sum("total_fwd_bytes").alias("total_fwd_bytes"),
        F.sum("total_bwd_bytes").alias("total_bwd_bytes"),
        F.sum("total_packets").alias("total_packets"),
        F.avg("computed_avg_packet_size").alias("avg_packet_size"),
        F.avg("flow_duration").alias("avg_flow_duration"),
        F.sum(F.when(F.col("is_failed_or_reset"), 1).otherwise(0)).alias("failed_or_reset_count"),
        F.sort_array(F.collect_set("protocol_name")).alias("protocols_used"),
        F.min("event_timestamp").alias("first_seen"),
        F.max("event_timestamp").alias("last_seen"),
    )


def aggregate_by_time_window(df: DataFrame) -> DataFrame:
    """Aggregate network traffic volume across 5-minute time windows."""
    return (
        df.groupBy("time_window_5m")
        .agg(
            F.count("*").alias("connection_frequency"),
            F.countDistinct("source_ip").alias("distinct_src_ips"),
            F.countDistinct("destination_ip").alias("distinct_dst_ips"),
            F.countDistinct("destination_port").alias("distinct_dst_ports"),
            F.sum("total_bytes").alias("total_bytes_transferred"),
            F.sum("total_packets").alias("total_packets"),
            F.avg("computed_avg_packet_size").alias("avg_packet_size"),
            F.sum(F.when(F.col("is_failed_or_reset"), 1).otherwise(0)).alias("failed_or_reset_count"),
        )
        .orderBy("time_window_5m")
    )


def aggregate_by_destination_port(df: DataFrame) -> DataFrame:
    """Aggregate traffic patterns per destination port and protocol."""
    return df.groupBy("destination_port", "protocol_name").agg(
        F.count("*").alias("connection_frequency"),
        F.countDistinct("source_ip").alias("distinct_src_ips"),
        F.countDistinct("destination_ip").alias("distinct_dst_ips"),
        F.sum("total_bytes").alias("total_bytes_transferred"),
        F.avg("computed_avg_packet_size").alias("avg_packet_size"),
    )
