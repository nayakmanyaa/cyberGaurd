"""Verify that Python, NumPy, PySpark DataFrame API, and CyberGuard config work."""

from __future__ import annotations

import platform
import sys
from pathlib import Path

import numpy as np
import pyspark
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import load_config
from config.spark_session import create_spark_session


def main() -> int:
    cfg = load_config()
    print(f"Python:  {platform.python_version()} ({sys.executable})")
    print(f"NumPy:   {np.__version__}")
    print(f"PySpark: {pyspark.__version__}")
    print(f"MongoDB: {cfg.mongo.uri} / db={cfg.mongo.database} (config only)")
    print(f"Neo4j:   {cfg.neo4j.uri} / db={cfg.neo4j.database} / user={cfg.neo4j.user} (config only)")

    spark = create_spark_session(cfg.spark)
    try:
        rows = [("10.0.0.1", "10.0.0.2", 80), ("10.0.0.1", "10.0.0.3", 443)]
        df = spark.createDataFrame(rows, ["src_ip", "dst_ip", "dst_port"])
        summary = df.groupBy("src_ip").agg(F.count("dst_port").alias("flow_count")).collect()
        assert len(summary) == 1 and summary[0]["flow_count"] == 2
        print(f"SparkSession OK: version={spark.version}, master={spark.sparkContext.master}")
        print(f"DataFrame test OK: src_ip={summary[0]['src_ip']}, flow_count={summary[0]['flow_count']}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
