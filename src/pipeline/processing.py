from __future__ import annotations

import argparse
import csv
import shutil
import sqlite3
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window

from pipeline.windowing import build_processing_window


ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.IntegerType(), nullable=False),
        T.StructField("order_id", T.IntegerType(), nullable=False),
        T.StructField("customer_id", T.IntegerType(), nullable=False),
        T.StructField("status", T.StringType(), nullable=False),
        T.StructField("amount", T.DoubleType(), nullable=False),
        T.StructField("business_date", T.StringType(), nullable=False),
        T.StructField("ingested_at", T.StringType(), nullable=False),
        T.StructField("source_file", T.StringType(), nullable=False),
    ]
)


def build_spark(app_name: str = "orders-temporal-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def read_orders_csv(spark: SparkSession, input_path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(ORDERS_SCHEMA)
        .csv(str(input_path))
        .withColumn("business_date", F.to_date("business_date"))
    )


def deduplicate_orders(orders: DataFrame) -> DataFrame:
    latest_event = Window.partitionBy("order_id", "business_date").orderBy(
        F.to_timestamp("ingested_at").desc(),
        F.col("event_id").desc(),
    )

    return (
        orders.withColumn("row_number", F.row_number().over(latest_event))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def aggregate_orders(orders: DataFrame, process_date: str) -> DataFrame:
    return (
        orders.groupBy("business_date", "status")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.countDistinct("customer_id").alias("customers_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.max("ingested_at").alias("max_ingested_at"),
        )
        .withColumn("processed_for_date", F.lit(process_date).cast("date"))
        .withColumn("processed_at", F.current_timestamp())
        .select(
            "business_date",
            "status",
            "orders_count",
            "customers_count",
            "total_amount",
            "max_ingested_at",
            "processed_for_date",
            "processed_at",
        )
    )


def replace_parquet_partitions(summary: DataFrame, output_path: Path, process_date: str, lookback_days: int) -> None:
    window = build_processing_window(process_date, lookback_days)
    output_path.mkdir(parents=True, exist_ok=True)

    current = window.start_date
    while current <= window.end_date:
        partition_path = output_path / f"business_date={current.isoformat()}"
        if partition_path.exists():
            shutil.rmtree(partition_path)
        current = current.fromordinal(current.toordinal() + 1)

    summary.write.mode("append").partitionBy("business_date").parquet(str(output_path))


def collect_summary_rows(summary: DataFrame) -> list[tuple[str, str, int, int, float, str | None, str, str | None]]:
    def format_temporal(value: object) -> str | None:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat(sep=" ")  # type: ignore[call-arg]
        return str(value)

    return [
        (
            row.business_date.isoformat(),
            row.status,
            int(row.orders_count),
            int(row.customers_count),
            float(row.total_amount),
            format_temporal(row.max_ingested_at),
            row.processed_for_date.isoformat(),
            format_temporal(row.processed_at),
        )
        for row in summary.collect()
    ]


def replace_csv_window(
    rows: list[tuple[str, str, int, int, float, str | None, str, str | None]],
    output_path: Path,
    process_date: str,
    lookback_days: int,
) -> None:
    window = build_processing_window(process_date, lookback_days)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "business_date",
        "status",
        "orders_count",
        "customers_count",
        "total_amount",
        "max_ingested_at",
        "processed_for_date",
        "processed_at",
    ]

    existing_rows: list[dict[str, str]] = []
    if output_path.exists():
        with output_path.open(encoding="utf-8", newline="") as file:
            existing_rows = [
                row
                for row in csv.DictReader(file)
                if not (window.start_iso <= row["business_date"] <= window.end_iso)
            ]

    new_rows = [dict(zip(columns, row)) for row in rows]
    all_rows = sorted(existing_rows + new_rows, key=lambda row: (row["business_date"], row["status"]))

    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(all_rows)


def replace_sqlite_window(
    rows: list[tuple[str, str, int, int, float, str | None, str, str | None]],
    db_path: Path,
    process_date: str,
    lookback_days: int,
) -> None:
    window = build_processing_window(process_date, lookback_days)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_daily_summary (
                business_date TEXT NOT NULL,
                status TEXT NOT NULL,
                orders_count INTEGER NOT NULL,
                customers_count INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                max_ingested_at TEXT,
                processed_for_date TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                PRIMARY KEY (business_date, status)
            )
            """
        )
        conn.execute(
            """
            DELETE FROM orders_daily_summary
            WHERE business_date BETWEEN ? AND ?
            """,
            (window.start_iso, window.end_iso),
        )
        conn.executemany(
            """
            INSERT INTO orders_daily_summary (
                business_date, status, orders_count, customers_count,
                total_amount, max_ingested_at, processed_for_date, processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def process_orders(
    input_path: Path,
    parquet_output_path: Path,
    csv_output_path: Path,
    mart_db_path: Path,
    process_date: str,
    lookback_days: int,
    write_parquet: bool = False,
) -> None:
    spark = build_spark()
    try:
        orders = read_orders_csv(spark, input_path)
        window = build_processing_window(process_date, lookback_days)
        windowed_orders = orders.filter(
            (F.col("business_date") >= F.lit(window.start_iso).cast("date"))
            & (F.col("business_date") <= F.lit(window.end_iso).cast("date"))
        )
        deduplicated = deduplicate_orders(windowed_orders)
        summary = aggregate_orders(deduplicated, process_date)
        rows = collect_summary_rows(summary)
        replace_csv_window(rows, csv_output_path, process_date, lookback_days)
        replace_sqlite_window(rows, mart_db_path, process_date, lookback_days)
        if write_parquet:
            replace_parquet_partitions(summary, parquet_output_path, process_date, lookback_days)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Process orders with PySpark.")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--parquet-output-path", default="data/curated/orders_daily_summary")
    parser.add_argument("--csv-output-path", default="data/curated/orders_daily_summary.csv")
    parser.add_argument("--mart-db-path", default="data/curated/orders_mart.db")
    parser.add_argument("--process-date", required=True)
    parser.add_argument("--lookback-days", type=int, default=2)
    parser.add_argument("--write-parquet", action="store_true")
    args = parser.parse_args()

    process_orders(
        input_path=Path(args.input_path),
        parquet_output_path=Path(args.parquet_output_path),
        csv_output_path=Path(args.csv_output_path),
        mart_db_path=Path(args.mart_db_path),
        process_date=args.process_date,
        lookback_days=args.lookback_days,
        write_parquet=args.write_parquet,
    )
    print("orders_daily_summary updated")


if __name__ == "__main__":
    main()
