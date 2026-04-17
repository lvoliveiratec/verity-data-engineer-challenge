import csv

from pipeline.extract_orders import export_orders_window
from pipeline.seed_orders import seed_database


def test_export_orders_window_includes_late_records(tmp_path):
    db_path = tmp_path / "orders.db"
    output_path = tmp_path / "orders_window.csv"
    seed_database(db_path, process_date="2026-03-16", reset=True)

    row_count = export_orders_window(
        db_path=db_path,
        output_path=output_path,
        process_date="2026-03-16",
        lookback_days=2,
    )

    with output_path.open(encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    assert row_count == 6
    assert {row["order_id"] for row in rows} == {"1001", "1002", "1003", "1004"}
