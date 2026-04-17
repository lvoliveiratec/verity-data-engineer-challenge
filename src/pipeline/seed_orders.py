from __future__ import annotations

import argparse
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path


def parse_process_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_sample_orders(process_date: str) -> list[tuple[int, int, int, str, float, str, str, str]]:
    reference_date = parse_process_date(process_date)
    business_date = reference_date - timedelta(days=1)
    late_business_date = reference_date - timedelta(days=2)
    event_prefix = int(reference_date.strftime("%Y%m%d")) * 10

    return [
        (
            event_prefix + 1,
            1001,
            501,
            "paid",
            120.00,
            business_date.isoformat(),
            f"{business_date.isoformat()} 08:00:00",
            "batch_001",
        ),
        (
            event_prefix + 2,
            1002,
            502,
            "paid",
            80.00,
            business_date.isoformat(),
            f"{business_date.isoformat()} 08:01:00",
            "batch_001",
        ),
        (
            event_prefix + 3,
            1001,
            501,
            "paid",
            120.00,
            business_date.isoformat(),
            f"{business_date.isoformat()} 08:05:00",
            "batch_001_retry",
        ),
        (
            event_prefix + 4,
            1003,
            503,
            "pending",
            200.00,
            business_date.isoformat(),
            f"{business_date.isoformat()} 08:10:00",
            "batch_001",
        ),
        (
            event_prefix + 5,
            1004,
            504,
            "paid",
            150.00,
            late_business_date.isoformat(),
            f"{reference_date.isoformat()} 09:00:00",
            "batch_late",
        ),
        (
            event_prefix + 6,
            1003,
            503,
            "paid",
            200.00,
            business_date.isoformat(),
            f"{reference_date.isoformat()} 09:05:00",
            "batch_update",
        ),
    ]


def seed_database(db_path: Path, process_date: str = "2026-03-16", reset: bool = False) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    orders = build_sample_orders(process_date)

    with sqlite3.connect(db_path) as conn:
        if reset:
            conn.execute("DROP TABLE IF EXISTS orders_raw")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_raw (
                event_id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                amount REAL NOT NULL,
                business_date TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                source_file TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT OR IGNORE INTO orders_raw (
                event_id, order_id, customer_id, status, amount,
                business_date, ingested_at, source_file
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            orders,
        )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the local SQLite source database.")
    parser.add_argument("--db-path", default="data/raw/orders.db")
    parser.add_argument("--process-date", default="2026-03-16")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    seed_database(Path(args.db_path), process_date=args.process_date, reset=args.reset)
    print(f"orders_raw ready at {args.db_path} for process_date={args.process_date}")


if __name__ == "__main__":
    main()
