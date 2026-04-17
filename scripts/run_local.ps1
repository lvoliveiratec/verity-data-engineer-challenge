param(
    [string]$ProcessDate = "2026-03-16",
    [int]$LookbackDays = 2,
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = Join-Path $ProjectRoot "src"
$env:MART_DB_PATH = Join-Path $ProjectRoot "data\curated\orders_mart.db"

& $PythonExe -m pipeline.seed_orders --db-path "$ProjectRoot\data\raw\orders.db" --reset
& $PythonExe -m pipeline.extract_orders `
    --db-path "$ProjectRoot\data\raw\orders.db" `
    --output-path "$ProjectRoot\data\staging\orders_raw_window_$ProcessDate.csv" `
    --process-date $ProcessDate `
    --lookback-days $LookbackDays
& $PythonExe -m pipeline.processing `
    --input-path "$ProjectRoot\data\staging\orders_raw_window_$ProcessDate.csv" `
    --parquet-output-path "$ProjectRoot\data\curated\orders_daily_summary" `
    --mart-db-path "$ProjectRoot\data\curated\orders_mart.db" `
    --process-date $ProcessDate `
    --lookback-days $LookbackDays

@'
import sqlite3
import os

db_path = os.environ["MART_DB_PATH"]
with sqlite3.connect(db_path) as conn:
    rows = conn.execute(
        """
        SELECT business_date, status, orders_count, customers_count, total_amount, max_ingested_at
        FROM orders_daily_summary
        ORDER BY business_date, status
        """
    ).fetchall()

for row in rows:
    print(row)
'@ | & $PythonExe -
