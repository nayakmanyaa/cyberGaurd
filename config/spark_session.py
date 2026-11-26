"""PySpark SparkSession builder for CyberGuard (DataFrame API only)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from config.settings import SparkConfig, load_config


def _prepare_runtime_env(spark_config: SparkConfig) -> None:
    """Ensure Java and Python paths are explicit before launching the Spark JVM."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if spark_config.java_home and not os.environ.get("JAVA_HOME"):
        if Path(spark_config.java_home).exists():
            os.environ["JAVA_HOME"] = spark_config.java_home


def create_spark_session(spark_config: SparkConfig | None = None) -> SparkSession:
    """Create or retrieve a configured PySpark SparkSession."""
    cfg = spark_config or load_config().spark
    _prepare_runtime_env(cfg)

    return (
        SparkSession.builder.appName(cfg.app_name)
        .master(cfg.master)
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
