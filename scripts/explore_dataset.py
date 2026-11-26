"""Read-only PySpark DataFrame dataset explorer for CIC-IDS2017 CSV files.

Inspects each CSV file independently without assuming a shared schema,
starting with a manageable sample by default and never loading full datasets
into Pandas.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, NumericType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import load_config
from config.spark_session import create_spark_session

NETWORK_IDENTITY_HINTS = (
    "flow id",
    "source ip",
    "src ip",
    "destination ip",
    "dst ip",
    "source port",
    "src port",
    "destination port",
    "dst port",
    "protocol",
    "timestamp",
    "label",
)


def find_csv_files(raw_dir: Path) -> list[Path]:
    """Return sorted CSV files found under `raw_dir` (recursively)."""
    if not raw_dir.exists():
        return []
    return sorted(p for p in raw_dir.rglob("*.csv") if p.is_file())


def _disambiguate_headers(columns: list[str]) -> list[str]:
    """Trim whitespace and rename duplicate headers deterministically."""
    seen: dict[str, int] = {}
    cleaned: list[str] = []
    for raw in columns:
        name = raw.strip()
        count = seen.get(name, 0) + 1
        seen[name] = count
        cleaned.append(name if count == 1 else f"{name}_{count}")
    return cleaned


def load_sample_df(
    spark: SparkSession,
    csv_path: Path,
    sample_rows: int | None,
    encoding: str,
) -> tuple[DataFrame, int]:
    """Read a CSV via PySpark DataFrame API and optionally limit to `sample_rows`."""
    full_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", encoding)
        .option("mode", "PERMISSIVE")
        .csv(str(csv_path))
    )
    total_rows = full_df.count()
    if sample_rows and sample_rows > 0 and total_rows > sample_rows:
        return full_df.limit(sample_rows), total_rows
    return full_df, total_rows


def inspect_file(
    spark: SparkSession,
    csv_path: Path,
    sample_rows: int | None,
    encoding: str,
) -> dict[str, Any]:
    """Inspect schema, data quality, identity fields, and labels for one CSV."""
    df, total_rows = load_sample_df(spark, csv_path, sample_rows, encoding)
    inspected_rows = df.count()

    raw_columns = list(df.columns)
    trimmed_columns = _disambiguate_headers(raw_columns)
    has_whitespace_headers = any(c != c.strip() for c in raw_columns)

    aliased_df = df.toDF(*trimmed_columns)
    schema_fields = [(f.name, f.dataType.simpleString()) for f in aliased_df.schema.fields]

    exprs = []
    numeric_cols = {
        f.name for f in aliased_df.schema.fields if isinstance(f.dataType, NumericType)
    }
    float_cols = {
        f.name
        for f in aliased_df.schema.fields
        if isinstance(f.dataType, (DoubleType, FloatType))
    }

    for idx, col_name in enumerate(trimmed_columns):
        col_ref = F.col(f"`{col_name}`")
        exprs.append(F.sum(F.when(col_ref.isNull(), 1).otherwise(0)).alias(f"null_{idx}"))
        if col_name in float_cols:
            exprs.append(
                F.sum(
                    F.when(
                        F.isnan(col_ref)
                        | (col_ref == F.lit(float("inf")))
                        | (col_ref == F.lit(float("-inf"))),
                        1,
                    ).otherwise(0)
                ).alias(f"badnum_{idx}")
            )

    stats_row = aliased_df.agg(*exprs).collect()[0].asDict() if exprs and inspected_rows > 0 else {}

    null_counts: dict[str, int] = {}
    non_finite_counts: dict[str, int] = {}
    for idx, col_name in enumerate(trimmed_columns):
        n_null = int(stats_row.get(f"null_{idx}", 0) or 0)
        if n_null > 0:
            null_counts[col_name] = n_null
        if col_name in float_cols:
            n_bad = int(stats_row.get(f"badnum_{idx}", 0) or 0)
            if n_bad > 0:
                non_finite_counts[col_name] = n_bad

    distinct_rows = aliased_df.dropDuplicates().count() if inspected_rows > 0 else 0
    duplicate_rows = inspected_rows - distinct_rows

    identity_cols = [
        c for c in trimmed_columns if any(hint in c.lower() for hint in NETWORK_IDENTITY_HINTS)
    ]

    timestamp_samples: list[str] = []
    if "Timestamp" in trimmed_columns and inspected_rows > 0:
        ts_rows = (
            aliased_df.select("Timestamp")
            .filter(F.col("Timestamp").isNotNull() & (F.trim(F.col("Timestamp")) != ""))
            .distinct()
            .limit(3)
            .collect()
        )
        timestamp_samples = [str(r[0]) for r in ts_rows]

    label_col = next((c for c in trimmed_columns if c.lower() == "label"), None)
    label_counts: list[tuple[str, int]] = []
    if label_col and inspected_rows > 0:
        label_rows = (
            aliased_df.groupBy(F.col(f"`{label_col}`"))
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        label_counts = [(str(r[0]), int(r[1])) for r in label_rows]

    return {
        "file_name": csv_path.name,
        "relative_path": str(csv_path.resolve().relative_to(PROJECT_ROOT.resolve())),
        "total_rows": total_rows,
        "inspected_rows": inspected_rows,
        "column_count": len(raw_columns),
        "has_whitespace_headers": has_whitespace_headers,
        "schema_fields": schema_fields,
        "numeric_column_count": len(numeric_cols),
        "identity_cols": identity_cols,
        "timestamp_samples": timestamp_samples,
        "null_counts": null_counts,
        "non_finite_counts": non_finite_counts,
        "duplicate_rows_in_sample": duplicate_rows,
        "label_counts": label_counts,
    }


def render_markdown_report(results: list[dict[str, Any]], sample_rows: int | None) -> str:
    """Format inspection findings into a Markdown report."""
    lines = [
        "# CIC-IDS2017 Dataset Exploration Report",
        "",
        f"- **Files inspected:** {len(results)}",
        f"- **Sample limit per file:** {sample_rows if sample_rows else 'Full file'}",
        "",
        "## 1. Per-File Overview",
        "",
        "| File Path | Total Rows | Inspected Rows | Columns | Header Spaces | Duplicates | Cols With Nulls | Non-Finite Cols |",
        "| :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: |",
    ]

    for r in results:
        lines.append(
            f"| `{r['relative_path']}` | {r['total_rows']:,} | {r['inspected_rows']:,} | "
            f"{r['column_count']} | {'Yes' if r['has_whitespace_headers'] else 'No'} | "
            f"{r['duplicate_rows_in_sample']:,} | {len(r['null_counts'])} | {len(r['non_finite_counts'])} |"
        )

    lines.extend(["", "## 2. Schema Comparison Across Files", ""])
    if results:
        baseline = [name for name, _ in results[0]["schema_fields"]]
        baseline_set = set(baseline)
        for r in results:
            cols = [name for name, _ in r["schema_fields"]]
            missing = [c for c in baseline if c not in cols]
            extra = [c for c in cols if c not in baseline_set]
            if not missing and not extra and cols == baseline:
                lines.append(f"- `{r['relative_path']}`: matches `{results[0]['relative_path']}` columns.")
            else:
                lines.append(
                    f"- `{r['relative_path']}`: **schema differs** (missing={missing}, extra={extra})."
                )

    lines.extend(["", "## 3. Identity, Port, Protocol, Timestamp & Label Fields", ""])
    for r in results:
        lines.append(f"### `{r['relative_path']}`")
        lines.append(f"- **Detected identity/label columns:** `{r['identity_cols']}`")
        if r["timestamp_samples"]:
            lines.append(f"- **Sample Timestamp strings:** `{r['timestamp_samples']}`")
        if r["null_counts"]:
            lines.append(f"- **Null counts:** `{r['null_counts']}`")
        if r["non_finite_counts"]:
            lines.append(f"- **NaN/Inf counts:** `{r['non_finite_counts']}`")
        if r["label_counts"]:
            lines.append("- **Label distribution (evaluation only, not an input feature):**")
            for label, cnt in r["label_counts"]:
                lines.append(f"  - `{label}`: {cnt:,}")
        lines.append("<details><summary>All columns and inferred Spark types</summary>\n")
        for name, dtype in r["schema_fields"]:
            lines.append(f"- `{name}` (`{dtype}`)")
        lines.append("\n</details>\n")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Explore CIC-IDS2017 CSV files with PySpark DataFrame API.")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Directory containing raw CSV files (defaults to RAW_DATA_DIR from config).",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=50000,
        help="Maximum rows per CSV to inspect initially (0 = inspect all rows).",
    )
    parser.add_argument(
        "--encoding",
        type=str,
        default="ISO-8859-1",
        help="CSV character encoding (default: ISO-8859-1 for CIC-IDS2017).",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "sample" / "dataset_exploration_report.md",
        help="Output path for the generated Markdown report.",
    )
    args = parser.parse_args()

    cfg = load_config()
    raw_dir = args.raw_dir or cfg.paths.raw_dir
    csv_files = find_csv_files(raw_dir)
    if not csv_files:
        print(f"No CSV files found under: {raw_dir}")
        return 1

    sample_limit = args.sample_rows if args.sample_rows > 0 else None
    spark = create_spark_session(cfg.spark)
    try:
        results = [inspect_file(spark, csv_path, sample_limit, args.encoding) for csv_path in csv_files]
    finally:
        spark.stop()

    report_md = render_markdown_report(results, sample_limit)
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(report_md, encoding="utf-8")
    print(f"Exploration report written to: {args.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
