from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from pipeline.windowing import build_processing_window


ORDER_COLUMNS = [
    "event_id",
    "order_id",
    "customer_id",
    "status",
    "amount",
    "business_date",
    "ingested_at",
    "source_file",
]


def export_orders_window(
    db_path: Path,
    output_path: Path,
    process_date: str,
    lookback_days: int,
) -> int:
    window = build_processing_window(process_date, lookback_days)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    query = f"""
        SELECT {", ".join(ORDER_COLUMNS)}
        FROM orders_raw
        WHERE business_date BETWEEN ? AND ?
          AND ingested_at < datetime(?, '+1 day')
        ORDER BY business_date, order_id, ingested_at, event_id
    """

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, (window.start_iso, window.end_iso, window.end_iso)).fetchall()

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(ORDER_COLUMNS)
        writer.writerows(rows)

    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a temporal window from orders_raw.")
    parser.add_argument("--db-path", default="data/raw/orders.db")
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--process-date", required=True)
    parser.add_argument("--lookback-days", type=int, default=2)
    args = parser.parse_args()

    row_count = export_orders_window(
        db_path=Path(args.db_path),
        output_path=Path(args.output_path),
        process_date=args.process_date,
        lookback_days=args.lookback_days,
    )
    print(f"exported {row_count} rows to {args.output_path}")


if __name__ == "__main__":
    main()
