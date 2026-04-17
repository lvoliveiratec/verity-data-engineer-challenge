import os

import pytest

from pipeline.processing import aggregate_orders, build_spark, deduplicate_orders


pytestmark = pytest.mark.filterwarnings("ignore")
requires_spark_on_windows = pytest.mark.skipif(
    os.name == "nt" and os.environ.get("RUN_SPARK_TESTS") != "1",
    reason="Spark integration tests are opt-in on Windows because local PySpark workers are environment-sensitive.",
)


@requires_spark_on_windows
def test_deduplicate_orders_keeps_latest_event_per_order_and_business_date():
    spark = build_spark("test-deduplicate-orders")
    try:
        rows = [
            (1, 1003, 503, "pending", 200.0, "2026-03-15", "2026-03-15 08:10:00", "batch_001"),
            (6, 1003, 503, "paid", 200.0, "2026-03-15", "2026-03-16 09:05:00", "batch_update"),
        ]
        df = spark.createDataFrame(
            rows,
            "event_id int, order_id int, customer_id int, status string, amount double, business_date string, ingested_at string, source_file string",
        )
        df = df.selectExpr(
            "event_id",
            "order_id",
            "customer_id",
            "status",
            "amount",
            "to_date(business_date) as business_date",
            "to_timestamp(ingested_at) as ingested_at",
            "source_file",
        )

        result = deduplicate_orders(df).collect()

        assert len(result) == 1
        assert result[0].event_id == 6
        assert result[0].status == "paid"
    finally:
        spark.stop()


@requires_spark_on_windows
def test_aggregate_orders_builds_daily_status_summary():
    spark = build_spark("test-aggregate-orders")
    try:
        rows = [
            (1001, 501, "paid", 120.0, "2026-03-15", "2026-03-15 08:00:00"),
            (1002, 502, "paid", 80.0, "2026-03-15", "2026-03-15 08:01:00"),
        ]
        df = spark.createDataFrame(
            rows,
            "order_id int, customer_id int, status string, amount double, business_date string, ingested_at string",
        ).selectExpr(
            "order_id",
            "customer_id",
            "status",
            "amount",
            "to_date(business_date) as business_date",
            "to_timestamp(ingested_at) as ingested_at",
        )

        result = aggregate_orders(df, "2026-03-16").collect()

        assert len(result) == 1
        assert result[0].orders_count == 2
        assert result[0].customers_count == 2
        assert result[0].total_amount == 200.0
    finally:
        spark.stop()
