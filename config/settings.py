"""Environment-driven configuration for CyberGuard.

Loads settings from `.env` (when present) and OS environment variables without
hardcoding credentials or opening database connections during initialization.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class SparkConfig:
    """PySpark session configuration."""

    app_name: str
    master: str
    driver_memory: str
    shuffle_partitions: int
    java_home: str | None


@dataclass(frozen=True)
class MongoConfig:
    """MongoDB connection configuration (settings only; no client created here)."""

    uri: str
    database: str
    alerts_collection: str
    records_collection: str


@dataclass(frozen=True)
class Neo4jConfig:
    """Neo4j connection configuration (settings only; no driver created here)."""

    uri: str
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class PathConfig:
    """Project data directory paths."""

    raw_dir: Path
    processed_dir: Path
    sample_dir: Path


@dataclass(frozen=True)
class DetectionThresholds:
    """Configurable statistical and heuristic thresholds for later phases."""

    high_conn_freq_zscore: float
    high_traffic_vol_zscore: float
    port_scan_unique_ports: int
    failed_conn_threshold: int
    outbound_ratio_threshold: float


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration container."""

    spark: SparkConfig
    mongo: MongoConfig
    neo4j: Neo4jConfig
    paths: PathConfig
    thresholds: DetectionThresholds


def _resolve_path(raw_path: str) -> Path:
    """Resolve a configured path relative to the project root if not absolute."""
    candidate = Path(raw_path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def load_config(env_file: Path | None = None) -> AppConfig:
    """Load application configuration from `.env` and environment variables."""
    target_env = env_file or (PROJECT_ROOT / ".env")
    if target_env.exists():
        load_dotenv(dotenv_path=target_env, override=False)

    java_home = os.getenv("JAVA_HOME") or None

    spark = SparkConfig(
        app_name=os.getenv("SPARK_APP_NAME", "CyberGuard-Analytics"),
        master=os.getenv("SPARK_MASTER", "local[*]"),
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")),
        java_home=java_home,
    )

    mongo = MongoConfig(
        uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        database=os.getenv("MONGO_DB_NAME", "cyberguard"),
        alerts_collection=os.getenv("MONGO_ALERTS_COLLECTION", "alerts"),
        records_collection=os.getenv("MONGO_RECORDS_COLLECTION", "investigation_records"),
    )

    neo4j = Neo4jConfig(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", ""),
        database=os.getenv("NEO4J_DATABASE", "neo4j"),
    )

    paths = PathConfig(
        raw_dir=_resolve_path(os.getenv("RAW_DATA_DIR", "data/raw")),
        processed_dir=_resolve_path(os.getenv("PROCESSED_DATA_DIR", "data/processed")),
        sample_dir=_resolve_path(os.getenv("SAMPLE_DATA_DIR", "data/sample")),
    )

    thresholds = DetectionThresholds(
        high_conn_freq_zscore=float(os.getenv("HIGH_CONN_FREQ_ZSCORE", "2.5")),
        high_traffic_vol_zscore=float(os.getenv("HIGH_TRAFFIC_VOL_ZSCORE", "2.5")),
        port_scan_unique_ports=int(os.getenv("PORT_SCAN_UNIQUE_PORTS_THRESHOLD", "20")),
        failed_conn_threshold=int(os.getenv("FAILED_CONN_THRESHOLD", "10")),
        outbound_ratio_threshold=float(os.getenv("OUTBOUND_RATIO_THRESHOLD", "5.0")),
    )

    return AppConfig(
        spark=spark,
        mongo=mongo,
        neo4j=neo4j,
        paths=paths,
        thresholds=thresholds,
    )
