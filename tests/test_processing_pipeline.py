"""Unit and integration tests for the CyberGuard PySpark processing pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from config.settings import SparkConfig
from config.spark_session import create_spark_session
from ingestion.schema import RAW_TRAFFIC_COLUMNS
from processing.pipeline import run_processing_pipeline


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    cfg = SparkConfig(
        app_name="CyberGuard-Processing-Tests",
        master="local[1]",
        driver_memory="1g",
        shuffle_partitions=2,
        java_home=None,
    )
    session = create_spark_session(cfg)
    yield session
    session.stop()


def _build_csv_row(
    *,
    src_ip: str = "192.168.10.5",
    src_port: int = 51234,
    dst_ip: str = "172.16.0.1",
    dst_port: int = 80,
    protocol: int = 6,
    timestamp: str = "6/7/2017 9:01",
    duration: str = "1000000",
    fwd_pkts: str = "4",
    bwd_pkts: str = "2",
    fwd_bytes: str = "400",
    bwd_bytes: str = "200",
    flow_bytes_s: str = "600.0",
    flow_pkts_s: str = "6.0",
    syn_count: str = "1",
    rst_count: str = "0",
    ack_count: str = "1",
    label: str = "BENIGN",
) -> str:
    values = ["0"] * len(RAW_TRAFFIC_COLUMNS)
    col_idx = {}
    for idx, (_, clean_name, _) in enumerate(RAW_TRAFFIC_COLUMNS):
        col_idx[clean_name] = idx

    values[col_idx["flow_id"]] = f"{src_ip}-{dst_ip}-{src_port}-{dst_port}-{protocol}"
    values[col_idx["source_ip"]] = src_ip
    values[col_idx["source_port"]] = str(src_port)
    values[col_idx["destination_ip"]] = dst_ip
    values[col_idx["destination_port"]] = str(dst_port)
    values[col_idx["protocol"]] = str(protocol)
    values[col_idx["timestamp_raw"]] = timestamp
    values[col_idx["flow_duration"]] = duration
    values[col_idx["total_fwd_packets"]] = fwd_pkts
    values[col_idx["total_bwd_packets"]] = bwd_pkts
    values[col_idx["total_fwd_bytes"]] = fwd_bytes
    values[col_idx["total_bwd_bytes"]] = bwd_bytes
    values[col_idx["flow_bytes_per_sec"]] = flow_bytes_s
    values[col_idx["flow_packets_per_sec"]] = flow_pkts_s
    values[col_idx["syn_flag_count"]] = syn_count
    values[col_idx["rst_flag_count"]] = rst_count
    values[col_idx["ack_flag_count"]] = ack_count
    values[col_idx["label"]] = label
    return ",".join(values)


def test_processing_pipeline_cleans_dedups_and_writes_parquet(
    tmp_path: Path, spark: SparkSession
) -> None:
    raw_dir = tmp_path / "raw" / "TrafficLabelling"
    raw_dir.mkdir(parents=True)
    processed_dir = tmp_path / "processed"

    header = ",".join(f" {raw_name}" for raw_name, _, _ in RAW_TRAFFIC_COLUMNS)
    row_valid_1 = _build_csv_row(
        src_ip="192.168.10.5",
        dst_ip="205.174.165.68",
        dst_port=80,
        timestamp="03/07/2017 08:56:46",
        label="BENIGN",
    )
    row_duplicate = row_valid_1
    row_inf_and_web = _build_csv_row(
        src_ip="205.174.165.73",
        dst_ip="192.168.10.50",
        dst_port=443,
        timestamp="6/7/2017 9:01",
        duration="0",
        flow_bytes_s="Infinity",
        flow_pkts_s="NaN",
        rst_count="1",
        label="Web Attack – Brute Force",
    )
    row_empty = ",".join([""] * len(RAW_TRAFFIC_COLUMNS))

    csv_file = raw_dir / "sample.csv"
    csv_file.write_bytes(
        "\n".join([header, row_valid_1, row_duplicate, row_inf_and_web, row_empty]).encode(
            "cp1252"
        )
    )

    summary = run_processing_pipeline(
        raw_dir=tmp_path / "raw",
        processed_dir=processed_dir,
        spark=spark,
    )

    assert summary.files_processed == 1
    assert summary.input_record_count == 4
    assert summary.dropped_invalid_rows == 1
    assert summary.duplicates_removed == 1
    assert summary.output_record_count == 2
    assert summary.non_finite_or_null_numeric_before >= 2
    assert summary.missing_identity_after == 0
    assert summary.non_finite_or_null_numeric_after == 0
    assert summary.source_ip_agg_count == 2
    assert summary.ip_pair_agg_count == 2

    flows_df = spark.read.parquet(str(processed_dir / "cleaned_flows.parquet"))
    assert "fwd_header_length_dup" not in flows_df.columns
    assert "total_bytes" in flows_df.columns
    assert "computed_avg_packet_size" in flows_df.columns
    assert flows_df.filter(flows_df.label == "Web Attack - Brute Force").count() == 1

    ip_pairs_df = spark.read.parquet(str(processed_dir / "agg_ip_pairs.parquet"))
    assert ip_pairs_df.count() == 2
