"""Environment, configuration, and PySpark verification tests."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from pyspark.sql import functions as F

from config.settings import SparkConfig, load_config
from config.spark_session import create_spark_session


def test_load_config_reads_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "MONGO_URI=mongodb://localhost:27017",
                "MONGO_DB_NAME=cyberguard_test",
                "NEO4J_URI=bolt://localhost:7687",
                "NEO4J_USER=neo4j",
                "NEO4J_PASSWORD=test_secret",
                "SPARK_APP_NAME=CyberGuard-Test",
                "SPARK_MASTER=local[1]",
            ]
        ),
        encoding="utf-8",
    )

    for key in (
        "MONGO_URI",
        "MONGO_DB_NAME",
        "NEO4J_URI",
        "NEO4J_USER",
        "NEO4J_PASSWORD",
        "SPARK_APP_NAME",
        "SPARK_MASTER",
    ):
        monkeypatch.delenv(key, raising=False)

    cfg = load_config(env_file=env_file)

    assert cfg.mongo.uri == "mongodb://localhost:27017"
    assert cfg.mongo.database == "cyberguard_test"
    assert cfg.neo4j.uri == "bolt://localhost:7687"
    assert cfg.neo4j.user == "neo4j"
    assert cfg.neo4j.password == "test_secret"
    assert cfg.spark.app_name == "CyberGuard-Test"
    assert cfg.spark.master == "local[1]"


def test_numpy_available() -> None:
    values = np.array([10.0, 12.0, 14.0])
    assert float(np.mean(values)) == pytest.approx(12.0)


def test_pyspark_dataframe_session_runs() -> None:
    base_cfg = load_config().spark
    test_spark_cfg = SparkConfig(
        app_name="CyberGuard-EnvTest",
        master="local[1]",
        driver_memory="1g",
        shuffle_partitions=1,
        java_home=base_cfg.java_home,
    )

    spark = create_spark_session(test_spark_cfg)
    try:
        df = spark.createDataFrame(
            [("192.168.1.10", 22), ("192.168.1.10", 80), ("192.168.1.20", 443)],
            ["src_ip", "dst_port"],
        )
        agg = (
            df.groupBy("src_ip")
            .agg(F.countDistinct("dst_port").alias("unique_ports"))
            .filter(F.col("src_ip") == "192.168.1.10")
            .collect()
        )
        assert len(agg) == 1
        assert agg[0]["unique_ports"] == 2
    finally:
        spark.stop()
