"""Explicit PySpark schema and clean column definitions for CIC-IDS2017 TrafficLabelling."""

from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Exact 85-column positional layout verified across all 8 TrafficLabelling CSVs.
# Note: Raw CSV headers repeat "Fwd Header Length" at indices 40 and 61; we
# disambiguate index 61 as "fwd_header_length_dup" upon ingestion.
RAW_TRAFFIC_COLUMNS: list[tuple[str, str, object]] = [
    ("Flow ID", "flow_id", StringType()),
    ("Source IP", "source_ip", StringType()),
    ("Source Port", "source_port", IntegerType()),
    ("Destination IP", "destination_ip", StringType()),
    ("Destination Port", "destination_port", IntegerType()),
    ("Protocol", "protocol", IntegerType()),
    ("Timestamp", "timestamp_raw", StringType()),
    ("Flow Duration", "flow_duration", DoubleType()),
    ("Total Fwd Packets", "total_fwd_packets", DoubleType()),
    ("Total Backward Packets", "total_bwd_packets", DoubleType()),
    ("Total Length of Fwd Packets", "total_fwd_bytes", DoubleType()),
    ("Total Length of Bwd Packets", "total_bwd_bytes", DoubleType()),
    ("Fwd Packet Length Max", "fwd_pkt_len_max", DoubleType()),
    ("Fwd Packet Length Min", "fwd_pkt_len_min", DoubleType()),
    ("Fwd Packet Length Mean", "fwd_pkt_len_mean", DoubleType()),
    ("Fwd Packet Length Std", "fwd_pkt_len_std", DoubleType()),
    ("Bwd Packet Length Max", "bwd_pkt_len_max", DoubleType()),
    ("Bwd Packet Length Min", "bwd_pkt_len_min", DoubleType()),
    ("Bwd Packet Length Mean", "bwd_pkt_len_mean", DoubleType()),
    ("Bwd Packet Length Std", "bwd_pkt_len_std", DoubleType()),
    ("Flow Bytes/s", "flow_bytes_per_sec", DoubleType()),
    ("Flow Packets/s", "flow_packets_per_sec", DoubleType()),
    ("Flow IAT Mean", "flow_iat_mean", DoubleType()),
    ("Flow IAT Std", "flow_iat_std", DoubleType()),
    ("Flow IAT Max", "flow_iat_max", DoubleType()),
    ("Flow IAT Min", "flow_iat_min", DoubleType()),
    ("Fwd IAT Total", "fwd_iat_total", DoubleType()),
    ("Fwd IAT Mean", "fwd_iat_mean", DoubleType()),
    ("Fwd IAT Std", "fwd_iat_std", DoubleType()),
    ("Fwd IAT Max", "fwd_iat_max", DoubleType()),
    ("Fwd IAT Min", "fwd_iat_min", DoubleType()),
    ("Bwd IAT Total", "bwd_iat_total", DoubleType()),
    ("Bwd IAT Mean", "bwd_iat_mean", DoubleType()),
    ("Bwd IAT Std", "bwd_iat_std", DoubleType()),
    ("Bwd IAT Max", "bwd_iat_max", DoubleType()),
    ("Bwd IAT Min", "bwd_iat_min", DoubleType()),
    ("Fwd PSH Flags", "fwd_psh_flags", IntegerType()),
    ("Bwd PSH Flags", "bwd_psh_flags", IntegerType()),
    ("Fwd URG Flags", "fwd_urg_flags", IntegerType()),
    ("Bwd URG Flags", "bwd_urg_flags", IntegerType()),
    ("Fwd Header Length", "fwd_header_length", DoubleType()),
    ("Bwd Header Length", "bwd_header_length", DoubleType()),
    ("Fwd Packets/s", "fwd_packets_per_sec", DoubleType()),
    ("Bwd Packets/s", "bwd_packets_per_sec", DoubleType()),
    ("Min Packet Length", "min_packet_length", DoubleType()),
    ("Max Packet Length", "max_packet_length", DoubleType()),
    ("Packet Length Mean", "packet_length_mean", DoubleType()),
    ("Packet Length Std", "packet_length_std", DoubleType()),
    ("Packet Length Variance", "packet_length_variance", DoubleType()),
    ("FIN Flag Count", "fin_flag_count", IntegerType()),
    ("SYN Flag Count", "syn_flag_count", IntegerType()),
    ("RST Flag Count", "rst_flag_count", IntegerType()),
    ("PSH Flag Count", "psh_flag_count", IntegerType()),
    ("ACK Flag Count", "ack_flag_count", IntegerType()),
    ("URG Flag Count", "urg_flag_count", IntegerType()),
    ("CWE Flag Count", "cwe_flag_count", IntegerType()),
    ("ECE Flag Count", "ece_flag_count", IntegerType()),
    ("Down/Up Ratio", "down_up_ratio", DoubleType()),
    ("Average Packet Size", "average_packet_size", DoubleType()),
    ("Avg Fwd Segment Size", "avg_fwd_segment_size", DoubleType()),
    ("Avg Bwd Segment Size", "avg_bwd_segment_size", DoubleType()),
    ("Fwd Header Length", "fwd_header_length_dup", DoubleType()),
    ("Fwd Avg Bytes/Bulk", "fwd_avg_bytes_bulk", DoubleType()),
    ("Fwd Avg Packets/Bulk", "fwd_avg_packets_bulk", DoubleType()),
    ("Fwd Avg Bulk Rate", "fwd_avg_bulk_rate", DoubleType()),
    ("Bwd Avg Bytes/Bulk", "bwd_avg_bytes_bulk", DoubleType()),
    ("Bwd Avg Packets/Bulk", "bwd_avg_packets_bulk", DoubleType()),
    ("Bwd Avg Bulk Rate", "bwd_avg_bulk_rate", DoubleType()),
    ("Subflow Fwd Packets", "subflow_fwd_packets", DoubleType()),
    ("Subflow Fwd Bytes", "subflow_fwd_bytes", DoubleType()),
    ("Subflow Bwd Packets", "subflow_bwd_packets", DoubleType()),
    ("Subflow Bwd Bytes", "subflow_bwd_bytes", DoubleType()),
    ("Init_Win_bytes_forward", "init_win_bytes_forward", IntegerType()),
    ("Init_Win_bytes_backward", "init_win_bytes_backward", IntegerType()),
    ("act_data_pkt_fwd", "act_data_pkt_fwd", DoubleType()),
    ("min_seg_size_forward", "min_seg_size_forward", DoubleType()),
    ("Active Mean", "active_mean", DoubleType()),
    ("Active Std", "active_std", DoubleType()),
    ("Active Max", "active_max", DoubleType()),
    ("Active Min", "active_min", DoubleType()),
    ("Idle Mean", "idle_mean", DoubleType()),
    ("Idle Std", "idle_std", DoubleType()),
    ("Idle Max", "idle_max", DoubleType()),
    ("Idle Min", "idle_min", DoubleType()),
    ("Label", "label", StringType()),
]

CLEAN_COLUMN_NAMES: list[str] = [clean for _, clean, _ in RAW_TRAFFIC_COLUMNS]

TRAFFIC_LABELLING_SCHEMA = StructType(
    [StructField(clean, dtype, True) for _, clean, dtype in RAW_TRAFFIC_COLUMNS]
)

IDENTITY_COLUMNS: tuple[str, ...] = (
    "flow_id",
    "source_ip",
    "source_port",
    "destination_ip",
    "destination_port",
    "protocol",
    "timestamp_raw",
)

NUMERIC_DOUBLE_COLUMNS: tuple[str, ...] = tuple(
    clean for _, clean, dtype in RAW_TRAFFIC_COLUMNS if isinstance(dtype, DoubleType)
)

NUMERIC_INT_COLUMNS: tuple[str, ...] = tuple(
    clean for _, clean, dtype in RAW_TRAFFIC_COLUMNS if isinstance(dtype, IntegerType)
)

# Kept strictly for post-detection evaluation, never used as detection input features.
EVALUATION_LABEL_COLUMN = "label"
