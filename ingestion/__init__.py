"""Batch ingestion package for CIC-IDS2017 TrafficLabelling CSV files."""

from ingestion.csv_loader import discover_traffic_csvs, read_traffic_csvs
from ingestion.schema import (
    CLEAN_COLUMN_NAMES,
    EVALUATION_LABEL_COLUMN,
    IDENTITY_COLUMNS,
    RAW_TRAFFIC_COLUMNS,
    TRAFFIC_LABELLING_SCHEMA,
)

__all__ = [
    "CLEAN_COLUMN_NAMES",
    "EVALUATION_LABEL_COLUMN",
    "IDENTITY_COLUMNS",
    "RAW_TRAFFIC_COLUMNS",
    "TRAFFIC_LABELLING_SCHEMA",
    "discover_traffic_csvs",
    "read_traffic_csvs",
]
