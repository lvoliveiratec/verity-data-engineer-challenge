from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_HOME = os.environ.get("PROJECT_HOME", "/opt/airflow")
PYTHONPATH = f"{PROJECT_HOME}/src"
PROCESS_DATE = "{{ dag_run.conf.get('process_date', ds) if dag_run and dag_run.conf else ds }}"
LOOKBACK_DAYS = "{{ params.lookback_days }}"
STAGING_FILE = f"{PROJECT_HOME}/data/staging/orders_raw_window_{PROCESS_DATE}.csv"


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="orders_daily_summary",
    description="Builds an idempotent analytical summary from orders_raw.",
    default_args=default_args,
    start_date=datetime(2026, 3, 14),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["verity", "pyspark", "orders"],
    params={
        "lookback_days": 2,
    },
) as dag:
    seed_source = BashOperator(
        task_id="seed_sqlite_source",
        bash_command=(
            f"PYTHONPATH={PYTHONPATH} python {PROJECT_HOME}/src/pipeline/seed_orders.py "
            f"--db-path {PROJECT_HOME}/data/raw/orders.db "
            f"--process-date {PROCESS_DATE}"
        ),
    )

    extract_window = BashOperator(
        task_id="extract_temporal_window",
        bash_command=(
            f"PYTHONPATH={PYTHONPATH} python -m pipeline.extract_orders "
            f"--db-path {PROJECT_HOME}/data/raw/orders.db "
            f"--output-path {STAGING_FILE} "
            f"--process-date {PROCESS_DATE} "
            f"--lookback-days {LOOKBACK_DAYS}"
        ),
    )

    process_with_spark = BashOperator(
        task_id="process_daily_summary_with_pyspark",
        bash_command=(
            f"PYTHONPATH={PYTHONPATH} python -m pipeline.processing "
            f"--input-path {STAGING_FILE} "
            f"--parquet-output-path {PROJECT_HOME}/data/curated/orders_daily_summary "
            f"--mart-db-path {PROJECT_HOME}/data/curated/orders_mart.db "
            f"--process-date {PROCESS_DATE} "
            f"--lookback-days {LOOKBACK_DAYS}"
        ),
    )

    seed_source >> extract_window >> process_with_spark
